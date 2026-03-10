from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.routers.chat import router as chat_router
from app.api.routers.checklist import router as checklist_router
from app.api.routers.easy_contract import router as easy_contract_router
from app.bootstrap import create_container
from app.logging_config import setup_json_logging

# JSON 로깅 설정 (Promtail/Loki 호환)
setup_json_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.container = await create_container()
    await app.state.container.startup()
    try:
        yield
    finally:
        await app.state.container.aclose()


app = FastAPI(
    title="DojangKok AI Server",
    description="AI service for DojangKok",
    version="0.1.0",
    lifespan=lifespan,
)

# Prometheus 메트릭 설정
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

app.include_router(checklist_router)
app.include_router(easy_contract_router)
app.include_router(chat_router)


@app.get("/health")
def health_check():
    return {"status": "ok"}
