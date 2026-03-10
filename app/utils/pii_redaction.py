from __future__ import annotations
import re

_PHONE = re.compile(r"\b(01[016789])[-\s]?(\d{3,4})[-\s]?(\d{4})\b")
_PHONE2 = re.compile(r"\b(0\d{1,2})[-\s]?(\d{3,4})[-\s]?(\d{4})\b")

# 주민등록번호: YYMMDD-XXXXXXX 또는 YYMMDDXXXXXXX
_RRN1 = re.compile(r"\b(\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01]))[-\s]?(\d{7})\b")

_BANK_WORDS = r"(은행|뱅크|농협|국민|신한|우리|하나|기업|카카오|케이|토스|새마을|우체국|수협|신협)"
_ACCOUNT1 = re.compile(rf"\b{_BANK_WORDS}\s*[:\-]?\s*(\d{{2,6}}[-\s]?\d{{2,6}}[-\s]?\d{{2,14}})\b")
_ACCOUNT2 = re.compile(r"\b(계좌번호|계좌)\s*[:\-]?\s*(\d{2,6}[-\s]?\d{2,6}[-\s]?\d{2,14})\b")
_ACCOUNT3 = re.compile(r"\b(\d{2,6}-\d{2,6}-\d{5,14})\b")

def redact_phone_and_account(text: str) -> str:
    if not text:
        return text
    out = text
    out = _PHONE.sub("[개인정보(연락처)]", out)
    out = _PHONE2.sub("[개인정보(연락처)]", out)
    out = _RRN1.sub("[개인정보(주민등록번호)]", out)
    out = _ACCOUNT1.sub("[개인정보(계좌)]", out)
    out = _ACCOUNT2.sub("[개인정보(계좌)]", out)
    out = _ACCOUNT3.sub("[개인정보(계좌)]", out)
    return out