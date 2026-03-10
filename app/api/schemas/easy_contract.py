from pydantic import BaseModel, Field


class EasyContractFileMeta(BaseModel):
    url: str = Field(..., description="S3 presigned URL (pdf/png/jpg)")
    filename: str | None = Field(default=None, description="원본 파일명(로그/추적용)")
    doc_type: str = Field(..., description="문서 타입 (contract/registry/building_register 등)")


class EasyContractRequest(BaseModel):
    files: list[EasyContractFileMeta]


class EasyContractAcceptedResponse(BaseModel):
    id: int
    message: str
