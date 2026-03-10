from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., description="사용자 질문")
    stream: bool = Field(default=True, description="true면 SSE 스트리밍, false면 단건 응답")
    include_corpus: bool = Field(default=True, description="분쟁사례 코퍼스 검색 포함 여부")
    top_k_contract: int = Field(default=8, ge=1, le=20, description="계약서 벡터 검색 개수")
    top_k_corpus: int = Field(default=5, ge=1, le=20, description="코퍼스 벡터 검색 개수")


class ChatSseMeta(BaseModel):
    easy_contract_id: int
    contract_hits: int
    corpus_hits: int
