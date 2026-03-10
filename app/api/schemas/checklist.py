
from pydantic import BaseModel, Field


class ChecklistRequest(BaseModel):
    keywords: list[str] | None = Field(
        default=None, description="라이프스타일 키워드 목록(자연어 가능)"
    )


class ChecklistAcceptedResponse(BaseModel):
    id: str
    message: str


class ChecklistSyncResponse(BaseModel):
    checklists: list[str]
