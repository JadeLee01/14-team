from fastapi import Request

from app.bootstrap import AppContainer


def get_container(request: Request) -> AppContainer:
    return request.app.state.container
