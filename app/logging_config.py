import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from pythonjsonlogger import jsonlogger

# 환경 설정
APP_ENV = os.getenv("APP_ENV", "dev")

# 환경별 설정
CONFIG = {
    "local": {
        "log_path": "logs",
        "log_level": logging.DEBUG,
        "use_json": False,
    },
    "dev": {
        "log_path": "/var/log/dojangkok",
        "log_level": logging.INFO,
        "use_json": True,
    },
    "prod": {
        "log_path": "/var/log/dojangkok",
        "log_level": logging.INFO,
        "use_json": True,
    },
}


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """커스텀 JSON 포맷터 (app, env 필드 추가)"""

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["app"] = "dojangkok-ai"
        log_record["env"] = APP_ENV


def setup_json_logging() -> None:
    """로깅 설정 (Spring Boot logback 구조와 동일)

    - local: 패턴 형식, DEBUG, logs/
    - dev/prod: JSON 형식, INFO, /var/log/dojangkok/
    """
    config = CONFIG.get(APP_ENV, CONFIG["dev"])
    log_path = Path(config["log_path"]) / APP_ENV
    log_level = config["log_level"]
    use_json = config["use_json"]

    handlers = []

    # === 콘솔 Appender ===
    console_handler = logging.StreamHandler(sys.stdout)
    if use_json:
        console_handler.setFormatter(CustomJsonFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"},
            datefmt="%Y-%m-%dT%H:%M:%S%z",
            json_ensure_ascii=False,
        ))
    else:
        console_handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(threadName)s] %(levelname)-5s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S.%f"[:-3],
        ))
    handlers.append(console_handler)

    # === 파일 Appender (날짜별 롤링) ===
    try:
        log_path.mkdir(parents=True, exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            filename=log_path / "application.log",
            when="midnight",
            interval=1,
            backupCount=30,  # 30일 보관
            encoding="utf-8",
        )
        file_handler.suffix = "%Y-%m-%d"  # application.log.2025-01-29
        file_handler.namer = lambda name: name.replace(".log.", "-") + ".log"  # application-2025-01-29.log

        if use_json:
            file_handler.setFormatter(CustomJsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                rename_fields={"asctime": "timestamp", "levelname": "level"},
                datefmt="%Y-%m-%dT%H:%M:%S%z",
                json_ensure_ascii=False,
            ))
        else:
            file_handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s [%(threadName)s] %(levelname)-5s %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
        handlers.append(file_handler)
    except PermissionError:
        # 권한 없으면 파일 로깅 스킵 (콘솔만 사용)
        pass

    # === 루트 로거 설정 ===
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)
    for handler in handlers:
        root_logger.addHandler(handler)

    # === uvicorn 로거 설정 ===
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.setLevel(log_level)
        for handler in handlers:
            logger.addHandler(handler)

    # === app 로거 설정 (서비스 로거 포함) ===
    # app.services.*, app.api.* 등 모든 하위 로거에 핸들러 적용
    app_logger = logging.getLogger("app")
    app_logger.handlers.clear()
    app_logger.setLevel(log_level)
    app_logger.propagate = False  # root 로거로 전파 방지 (중복 로그 방지)
    for handler in handlers:
        app_logger.addHandler(handler)
