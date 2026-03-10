import re
from bs4 import BeautifulSoup

def normalize_text(s: str) -> str:
    s = s.replace("\u00a0", " ")
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n[ \t]+", "\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def table_to_text(table_tag) -> str:
    rows = []
    for tr in table_tag.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        cell_texts = []
        for c in cells:
            txt = c.get_text(separator=" ", strip=True)
            txt = re.sub(r"\s+", " ", txt).strip()
            cell_texts.append(txt)
        if cell_texts:
            rows.append("\t".join(cell_texts))
    return "\n".join(rows).strip()

def html_to_plain_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    for img in soup.find_all("img"):
        img.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")

    chunks = []
    for node in soup.find_all(["h1","h2","h3","p","header","footer","caption","table","figure"]):
        if node.name == "table":
            t = table_to_text(node)
            if t:
                chunks.append(t)
        else:
            t = node.get_text(separator=" ", strip=True)
            t = re.sub(r"\s+", " ", t).strip()
            if t:
                chunks.append(t)

    if not chunks:
        fallback = soup.get_text(separator="\n", strip=True)
        return normalize_text(fallback)

    return normalize_text("\n".join(chunks))

def extract_plain_text_from_upstage_json(data: dict) -> str:
    html = ""
    content = data.get("content", {})
    if isinstance(content, dict):
        html = content.get("html", "") or ""

    if not html:
        elements = data.get("elements", [])
        parts = []
        if isinstance(elements, list):
            for el in elements:
                c = (el or {}).get("content", {})
                h = (c or {}).get("html", "")
                if h:
                    parts.append(h)
        html = "\n".join(parts)

    if not html:
        return ""

    return html_to_plain_text(html)
