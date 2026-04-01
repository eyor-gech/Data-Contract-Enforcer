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
