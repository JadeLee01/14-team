# v2: Docker + MIG rolling update 배포 방식 적용
# CI/CD pipeline trigger test
import asyncio
import contextlib
import logging
from dataclasses import dataclass, field

import httpx
from aiolimiter import AsyncLimiter

from app.resources.http.client import create_async_http_client
from app.resources.ocr.upstage_client import UpstageDocumentParseClient
from app.resources.rabbitmq.client import QueueBinding, RabbitMQClient
from app.resources.rabbitmq.result_publisher import RabbitMQResultPublisher
from app.resources.vectorstore import ChromaVectorStore
from app.resources.vllm.client import VLLMClient
from app.services.callback_service import CallbackService
from app.services.cancel_registry import CancelRegistry
from app.services.chat_service import ChatService
from app.services.checklist_service import ChecklistService
from app.services.easy_contract_service import EasyContractService
from app.settings import settings
from app.workers.handlers.checklist_handler import ChecklistMessageHandler
from app.workers.handlers.easy_contract_cancel_handler import EasyContractCancelMessageHandler
from app.workers.handlers.easy_contract_handler import EasyContractMessageHandler
from app.workers.mq_worker import RabbitMQWorker

logger = logging.getLogger(__name__)


@dataclass
class AppContainer:
    http: httpx.AsyncClient
    vllm: VLLMClient
    callback: CallbackService
    chat_service: ChatService
    checklist_service: ChecklistService
    easy_contract_service: EasyContractService
    upstage: UpstageDocumentParseClient
    vector_store: ChromaVectorStore | None = None
    rabbitmq_client: RabbitMQClient | None = None
    rabbitmq_result_publisher: RabbitMQResultPublisher | None = None
    rabbitmq_bindings: list[QueueBinding] = field(default_factory=list)
    cancel_registry: CancelRegistry | None = None
    rabbitmq_worker: RabbitMQWorker | None = None
    cancel_cleanup_interval_sec: int = 60
    _cancel_cleanup_task: asyncio.Task | None = field(default=None, init=False, repr=False)

    async def startup(self) -> None:
        if self.rabbitmq_client is None:
            return
        await self.rabbitmq_client.connect()
        for binding in self.rabbitmq_bindings:
            await self.rabbitmq_client.ensure_binding(binding)
        if self.rabbitmq_worker is not None:
            await self.rabbitmq_worker.start()
        self._start_cancel_cleanup_task()

    async def aclose(self) -> None:
        await self._stop_cancel_cleanup_task()
        if self.rabbitmq_worker is not None:
            await self.rabbitmq_worker.stop()
        if self.rabbitmq_client is not None:
            await self.rabbitmq_client.close()
        await self.http.aclose()

    def _start_cancel_cleanup_task(self) -> None:
        if self.cancel_registry is None:
            return
        if self.cancel_cleanup_interval_sec <= 0:
            return
        if self._cancel_cleanup_task is not None and not self._cancel_cleanup_task.done():
            return

        self._cancel_cleanup_task = asyncio.create_task(
            self._cancel_cleanup_loop(),
            name="cancel-registry-cleanup",
        )
        logger.info(
            "취소 레지스트리 정리 작업 시작",
            extra={"interval_sec": self.cancel_cleanup_interval_sec},
        )

    async def _stop_cancel_cleanup_task(self) -> None:
        task = self._cancel_cleanup_task
        self._cancel_cleanup_task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        logger.info("취소 레지스트리 정리 작업 종료")

    async def _cancel_cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cancel_cleanup_interval_sec)
            if self.cancel_registry is not None:
                self.cancel_registry.cleanup_expired()


async def create_container() -> AppContainer:
    http = create_async_http_client(timeout_sec=settings.HTTP_TIMEOUT_SEC)

    vllm = VLLMClient(
        http=http,
        base_url=settings.VLLM_BASE_URL,
        api_key=settings.VLLM_API_KEY,
        model=settings.VLLM_MODEL,
        retry_max_attempts=settings.EXTERNAL_RETRY_MAX_ATTEMPTS,
        retry_backoff_base_sec=settings.EXTERNAL_RETRY_BACKOFF_BASE_SEC,
    )

    ocr_limiter = AsyncLimiter(1, time_period=2)
    upstage = UpstageDocumentParseClient(
        http=http,
        api_key=settings.UPSTAGE_API_KEY,
        url=settings.UPSTAGE_DOCUMENT_PARSE_URL,
        limiter=ocr_limiter,
        retry_max_attempts=settings.EXTERNAL_RETRY_MAX_ATTEMPTS,
        retry_backoff_base_sec=settings.EXTERNAL_RETRY_BACKOFF_BASE_SEC,
    )

    callback = CallbackService(
        http=http,
        base_url=settings.BACKEND_CALLBACK_BASE_URL,
        token=settings.BACKEND_INTERNAL_TOKEN,
    )

    vector_store: ChromaVectorStore | None = None
    try:
        vector_store = ChromaVectorStore(
            mode=settings.VECTOR_DB_MODE,
            persist_dir=settings.VECTOR_DB_DIR,
            host=settings.VECTOR_DB_HOST,
            port=settings.VECTOR_DB_PORT,
            contract_collection_name=settings.VECTOR_DB_COLLECTION_CONTRACT,
            corpus_collection_name=settings.VECTOR_DB_COLLECTION_CORPUS,
            embedding_model=settings.EMBEDDING_MODEL,
        )
        try:
            inserted = vector_store.ensure_corpus_loaded(corpus_jsonl_path=settings.CORPUS_JSONL_PATH)
            logger.info(
                "분쟁사례 코퍼스 준비 완료",
                extra={"inserted": inserted, "path": settings.CORPUS_JSONL_PATH},
            )
        except Exception:
            logger.exception("분쟁사례 코퍼스 자동 적재 실패", extra={"path": settings.CORPUS_JSONL_PATH})
    except Exception:
        logger.exception("벡터스토어 초기화 실패")

    chat_service = ChatService(vllm=vllm, vector_store=vector_store)
    checklist_service = ChecklistService(vllm=vllm)
    easy_contract_service = EasyContractService(vllm=vllm, ocr=upstage, vector_store=vector_store)

    rabbitmq_client: RabbitMQClient | None = None
    rabbitmq_result_publisher: RabbitMQResultPublisher | None = None
    rabbitmq_bindings: list[QueueBinding] = []
    cancel_registry: CancelRegistry | None = None
    rabbitmq_worker: RabbitMQWorker | None = None

    if settings.RABBITMQ_ENABLED:
        rabbitmq_client = RabbitMQClient(
            url=settings.RABBITMQ_URL,
            prefetch_count=settings.RABBITMQ_PREFETCH_COUNT,
            declare_passive=settings.RABBITMQ_DECLARE_PASSIVE,
        )
        cancel_registry = CancelRegistry(ttl_sec=settings.EASY_CONTRACT_CANCEL_TTL_SEC)
        rabbitmq_result_publisher = RabbitMQResultPublisher(
            client=rabbitmq_client,
            exchange_name=settings.RABBITMQ_RESULT_EXCHANGE,
            routing_key=settings.RABBITMQ_RESULT_ROUTING_KEY,
        )

        rabbitmq_bindings = [
            QueueBinding(
                exchange_name=settings.RABBITMQ_REQUEST_EXCHANGE_EASY_CONTRACT,
                queue_name=settings.RABBITMQ_REQUEST_QUEUE_EASY_CONTRACT,
                routing_key=settings.RABBITMQ_REQUEST_ROUTING_KEY_EASY_CONTRACT,
            ),
            QueueBinding(
                exchange_name=settings.RABBITMQ_REQUEST_EXCHANGE_CHECKLIST,
                queue_name=settings.RABBITMQ_REQUEST_QUEUE_CHECKLIST,
                routing_key=settings.RABBITMQ_REQUEST_ROUTING_KEY_CHECKLIST,
            ),
            QueueBinding(
                exchange_name=settings.RABBITMQ_CANCEL_EXCHANGE_EASY_CONTRACT,
                queue_name=settings.RABBITMQ_CANCEL_QUEUE_EASY_CONTRACT,
                routing_key=settings.RABBITMQ_CANCEL_ROUTING_KEY_EASY_CONTRACT,
            ),
            QueueBinding(
                exchange_name=settings.RABBITMQ_RESULT_EXCHANGE,
                queue_name=settings.RABBITMQ_RESULT_QUEUE,
                routing_key=settings.RABBITMQ_RESULT_ROUTING_KEY,
            ),
        ]

        easy_contract_handler = EasyContractMessageHandler(
            http=http,
            easy_contract_service=easy_contract_service,
            result_publisher=rabbitmq_result_publisher,
            cancel_registry=cancel_registry,
        )
        checklist_handler = ChecklistMessageHandler(
            checklist_service=checklist_service,
            result_publisher=rabbitmq_result_publisher,
        )
        easy_contract_cancel_handler = EasyContractCancelMessageHandler(cancel_registry=cancel_registry)
        rabbitmq_worker = RabbitMQWorker(
            client=rabbitmq_client,
            easy_contract_queue=settings.RABBITMQ_REQUEST_QUEUE_EASY_CONTRACT,
            checklist_queue=settings.RABBITMQ_REQUEST_QUEUE_CHECKLIST,
            easy_contract_cancel_queue=settings.RABBITMQ_CANCEL_QUEUE_EASY_CONTRACT,
            easy_contract_handler=easy_contract_handler.handle,
            checklist_handler=checklist_handler.handle,
            easy_contract_cancel_handler=easy_contract_cancel_handler.handle,
            result_publisher=rabbitmq_result_publisher,
            retry_max_attempts=settings.WORKER_RETRY_MAX_ATTEMPTS,
            retry_backoff_base_sec=settings.WORKER_RETRY_BACKOFF_BASE_SEC,
        )

    return AppContainer(
        http=http,
        vllm=vllm,
        callback=callback,
        chat_service=chat_service,
        checklist_service=checklist_service,
        easy_contract_service=easy_contract_service,
        upstage=upstage,
        vector_store=vector_store,
        rabbitmq_client=rabbitmq_client,
        rabbitmq_result_publisher=rabbitmq_result_publisher,
        rabbitmq_bindings=rabbitmq_bindings,
        cancel_registry=cancel_registry,
        rabbitmq_worker=rabbitmq_worker,
        cancel_cleanup_interval_sec=settings.EASY_CONTRACT_CANCEL_CLEANUP_INTERVAL_SEC,
    )
