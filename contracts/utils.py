from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import uuid
from collections.abc import Iterable, Iterator
from typing import Any


UUID_V4_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)


def stable_uuid_v4(seed: str) -> str:
    """
    Deterministically generate a v4 UUID by hashing seed and forcing version/variant bits.
    This preserves UUIDv4 format while remaining deterministic for reproducibility.
    """
    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    b = bytearray(digest[:16])
    b[6] = (b[6] & 0x0F) | 0x40  # version 4
    b[8] = (b[8] & 0x3F) | 0x80  # variant 10
    return str(uuid.UUID(bytes=bytes(b)))


def is_uuid_v4(value: Any) -> bool:
    return isinstance(value, str) and UUID_V4_RE.match(value) is not None


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso8601(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        v = value
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        return dt.datetime.fromisoformat(v)
    except Exception:
        return None


def to_iso8601_z(value: dt.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    value = value.astimezone(dt.timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return None
        if isinstance(x, int) and not isinstance(x, bool):
            return int(x)
        if isinstance(x, float) and x.is_integer():
            return int(x)
        if isinstance(x, str) and re.fullmatch(r"-?\\d+", x.strip() or "x"):
            return int(x.strip())
        return None
    except Exception:
        return None


def read_jsonl(path: str) -> Iterator[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                else:
                    yield {"_non_object": obj, "_line_no": line_no}
            except Exception as e:
                yield {"_parse_error": str(e), "_raw": line, "_line_no": line_no}


def write_jsonl(path: str, rows: Iterable[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def count_lines(path: str) -> int:
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except Exception:
        return 0


def top_level_keys_sample(path: str, sample_n: int = 50) -> list[str]:
    keys: set[str] = set()
    for i, row in enumerate(read_jsonl(path)):
        if i >= sample_n:
            break
        if isinstance(row, dict):
            keys.update(k for k in row.keys() if not k.startswith("_"))
    return sorted(keys)


@dataclasses.dataclass(frozen=True)
class Violation:
    vtype: str  # SCHEMA | SEMANTIC | STATISTICAL
    field: str
    severity: str  # LOW | MEDIUM | HIGH | CRITICAL
    count: int
    root_cause: str
    lineage_path: list[str]
    clause_id: str | None = None
    message: str | None = None
    samples: list[dict[str, Any]] | None = None


def safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)


def _pdf_escape(text: str) -> str:
    # PDF string literal escaping for parentheses/backslashes.
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def write_simple_pdf(
    pdf_path: str,
    *,
    title: str,
    lines: list[str],
    font_size: int = 10,
    page_width: int = 612,
    page_height: int = 792,
    margin_left: int = 54,
    margin_top: int = 54,
    line_height: int = 12,
    max_pages: int = 20,
) -> None:
    """
    Minimal, dependency-free PDF writer (Type1 Helvetica, text only).
    Deterministic output; intended for reports when reportlab isn't available.
    """
    out_dir = os.path.dirname(pdf_path) or "."
    os.makedirs(out_dir, exist_ok=True)

    usable_h = max(1, page_height - 2 * margin_top)
    lines_per_page = max(1, usable_h // max(1, line_height))
    pages = []
    for i in range(0, len(lines), lines_per_page):
        pages.append(lines[i : i + lines_per_page])
        if len(pages) >= max_pages:
            break
    if not pages:
        pages = [[]]

    header = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
    objects: list[bytes] = []

    # 1: Catalog, 2: Pages, 3..: Page/Contents pairs, last: Font
    font_obj_id = 3 + (len(pages) * 2)
    catalog_id = 1
    pages_id = 2

    def obj(n: int, body: bytes) -> bytes:
        return f"{n} 0 obj\n".encode("ascii") + body + b"\nendobj\n"

    # Font (Helvetica)
    font_obj = obj(font_obj_id, b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    page_obj_ids = [3 + (i * 2) for i in range(len(pages))]
    content_obj_ids = [pid + 1 for pid in page_obj_ids]

    kids = " ".join([f"{pid} 0 R" for pid in page_obj_ids]).encode("ascii")
    pages_obj = obj(pages_id, b"<< /Type /Pages /Kids [ " + kids + b" ] /Count " + str(len(pages)).encode("ascii") + b" >>")

    catalog_obj = obj(catalog_id, b"<< /Type /Catalog /Pages 2 0 R >>")

    objects.append(catalog_obj)
    objects.append(pages_obj)

    # Build pages + content streams.
    for page_idx, page_lines in enumerate(pages):
        pid = page_obj_ids[page_idx]
        cid = content_obj_ids[page_idx]
        # Content stream: title + lines.
        content_lines = [title] + [""] + page_lines if page_idx == 0 else page_lines
        text_ops = []
        text_ops.append("BT")
        text_ops.append(f"/F1 {int(font_size)} Tf")
        text_ops.append(f"{int(margin_left)} {int(page_height - margin_top)} Td")
        text_ops.append(f"{int(line_height)} TL")
        for ln in content_lines:
            s = _pdf_escape(str(ln)[:160])
            text_ops.append(f"({s}) Tj")
            text_ops.append("T*")
        text_ops.append("ET")
        stream = ("\n".join(text_ops) + "\n").encode("latin-1", errors="replace")
        content_obj = obj(cid, b"<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"endstream")

        page_obj = obj(
            pid,
            (
                b"<< /Type /Page /Parent 2 0 R "
                + b"/MediaBox [0 0 "
                + str(int(page_width)).encode("ascii")
                + b" "
                + str(int(page_height)).encode("ascii")
                + b"] "
                + b"/Contents "
                + f"{cid} 0 R".encode("ascii")
                + b" "
                + b"/Resources << /Font << /F1 "
                + f"{font_obj_id} 0 R".encode("ascii")
                + b" >> >> >>"
            ),
        )
        objects.append(page_obj)
        objects.append(content_obj)

    objects.append(font_obj)

    # Assemble with xref
    out = bytearray()
    out.extend(header)
    offsets = [0]  # object 0
    for o in objects:
        offsets.append(len(out))
        out.extend(o)

    xref_start = len(out)
    out.extend(f"xref\n0 {len(offsets)}\n".encode("ascii"))
    out.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        out.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    out.extend(b"trailer\n")
    out.extend(f"<< /Size {len(offsets)} /Root {catalog_id} 0 R >>\n".encode("ascii"))
    out.extend(b"startxref\n")
    out.extend(f"{xref_start}\n".encode("ascii"))
    out.extend(b"%%EOF\n")

    with open(pdf_path, "wb") as f:
        f.write(bytes(out))


def write_pdf_from_text(pdf_path: str, *, title: str, text: str) -> None:
    write_simple_pdf(pdf_path, title=title, lines=text.splitlines())
