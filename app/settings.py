import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _env_bool(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "")

    VLLM_BASE_URL: str = os.getenv("VLLM_BASE_URL", "")
    VLLM_API_KEY: str = os.getenv("VLLM_API_KEY", "")
    VLLM_MODEL: str = os.getenv("VLLM_MODEL", "LGAI-EXAONE/EXAONE-3.5-2.4B-Instruct")

    VLLM_LORA_ADAPTER_CHECKLIST: str = os.getenv("VLLM_LORA_ADAPTER_CHECKLIST", "")
    VLLM_LORA_ADAPTER_EASYCONTRACT: str = os.getenv("VLLM_LORA_ADAPTER_EASYCONTRACT", "")

    UPSTAGE_API_KEY: str = os.getenv("OCR_API", "")
    UPSTAGE_DOCUMENT_PARSE_URL: str = "https://api.upstage.ai/v1/document-digitization"

    BACKEND_CALLBACK_BASE_URL: str = os.getenv("BACKEND_CALLBACK_BASE_URL", "")
    BACKEND_INTERNAL_TOKEN: str = os.getenv("BACKEND_INTERNAL_TOKEN", "")

    HTTP_TIMEOUT_SEC: float = float(os.getenv("HTTP_TIMEOUT_SEC", "300.0"))
    EXTERNAL_RETRY_MAX_ATTEMPTS: int = int(os.getenv("EXTERNAL_RETRY_MAX_ATTEMPTS", "3"))
    EXTERNAL_RETRY_BACKOFF_BASE_SEC: float = float(os.getenv("EXTERNAL_RETRY_BACKOFF_BASE_SEC", "0.5"))

    RABBITMQ_ENABLED: bool = _env_bool("RABBITMQ_ENABLED", True)
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "")
    RABBITMQ_PREFETCH_COUNT: int = int(os.getenv("RABBITMQ_PREFETCH_COUNT", "3"))
    RABBITMQ_DECLARE_PASSIVE: bool = _env_bool("RABBITMQ_DECLARE_PASSIVE", False)
    WORKER_RETRY_MAX_ATTEMPTS: int = int(os.getenv("WORKER_RETRY_MAX_ATTEMPTS", "3"))
    WORKER_RETRY_BACKOFF_BASE_SEC: float = float(os.getenv("WORKER_RETRY_BACKOFF_BASE_SEC", "0.5"))

    RABBITMQ_REQUEST_EXCHANGE_EASY_CONTRACT: str = os.getenv("RABBITMQ_REQUEST_EXCHANGE_EASY_CONTRACT", "")
    RABBITMQ_REQUEST_QUEUE_EASY_CONTRACT: str = os.getenv("RABBITMQ_REQUEST_QUEUE_EASY_CONTRACT", "")
    RABBITMQ_REQUEST_ROUTING_KEY_EASY_CONTRACT: str = os.getenv("RABBITMQ_REQUEST_ROUTING_KEY_EASY_CONTRACT", "")

    RABBITMQ_REQUEST_EXCHANGE_CHECKLIST: str = os.getenv("RABBITMQ_REQUEST_EXCHANGE_CHECKLIST", "")
    RABBITMQ_REQUEST_QUEUE_CHECKLIST: str = os.getenv("RABBITMQ_REQUEST_QUEUE_CHECKLIST", "")
    RABBITMQ_REQUEST_ROUTING_KEY_CHECKLIST: str = os.getenv("RABBITMQ_REQUEST_ROUTING_KEY_CHECKLIST", "")

    RABBITMQ_CANCEL_EXCHANGE_EASY_CONTRACT: str = os.getenv("RABBITMQ_CANCEL_EXCHANGE_EASY_CONTRACT", "")
    RABBITMQ_CANCEL_QUEUE_EASY_CONTRACT: str = os.getenv("RABBITMQ_CANCEL_QUEUE_EASY_CONTRACT", "")
    RABBITMQ_CANCEL_ROUTING_KEY_EASY_CONTRACT: str = os.getenv("RABBITMQ_CANCEL_ROUTING_KEY_EASY_CONTRACT", "")

    RABBITMQ_RESULT_EXCHANGE: str = os.getenv("RABBITMQ_RESULT_EXCHANGE", "")
    RABBITMQ_RESULT_QUEUE: str = os.getenv("RABBITMQ_RESULT_QUEUE", "")
    RABBITMQ_RESULT_ROUTING_KEY: str = os.getenv("RABBITMQ_RESULT_ROUTING_KEY", "")

    EASY_CONTRACT_CANCEL_TTL_SEC: int = int(os.getenv("EASY_CONTRACT_CANCEL_TTL_SEC", "3600"))
    EASY_CONTRACT_CANCEL_CLEANUP_INTERVAL_SEC: int = int(os.getenv("EASY_CONTRACT_CANCEL_CLEANUP_INTERVAL_SEC", "60"))

    VECTOR_DB_MODE: str = os.getenv("VECTOR_DB_MODE", "local")
    VECTOR_DB_DIR: str = os.getenv("VECTOR_DB_DIR", "vector_db/chroma")
    VECTOR_DB_HOST: str = os.getenv("VECTOR_DB_HOST", "localhost")
    VECTOR_DB_PORT: int = int(os.getenv("VECTOR_DB_PORT", "8000"))
    VECTOR_DB_COLLECTION_CONTRACT: str = os.getenv("VECTOR_DB_COLLECTION_CONTRACT", "contract_docs")
    VECTOR_DB_COLLECTION_CORPUS: str = os.getenv("VECTOR_DB_COLLECTION_CORPUS", "dispute_cases")
    EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
    CORPUS_JSONL_PATH: str = os.getenv("CORPUS_JSONL_PATH", "corpus_data/corpus.jsonl")

settings = Settings()
