# AI Server — Multi-stage Build
# Base: python:3.10-slim, Package Manager: uv
# Target: linux/amd64 (GCP 서울 리전 T2A 미제공)

FROM python:3.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app/ ./app/

FROM python:3.10-slim

WORKDIR /app

RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && mkdir -p /var/log/dojangkok/prod \
    && chown -R appuser:appuser /var/log/dojangkok

COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/app /app/app
COPY --from=builder /app/pyproject.toml /app/pyproject.toml

ENV PATH="/app/.venv/bin:$PATH"
ENV APP_ENV=prod

USER appuser
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "debug"]
