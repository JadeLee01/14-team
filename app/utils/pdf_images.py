import fitz  # pymupdf


def pdf_bytes_to_png_pages(pdf_bytes: bytes, zoom: float = 2.0, max_pages: int | None = None) -> list[bytes]:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages: list[bytes] = []
    mat = fitz.Matrix(zoom, zoom)
    page_count = doc.page_count if max_pages is None else min(doc.page_count, max(0, max_pages))
    for i in range(page_count):
        page = doc.load_page(i)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        pages.append(pix.tobytes("png"))
    doc.close()
    return pages
