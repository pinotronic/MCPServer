from __future__ import annotations
import re
import unicodedata
from typing import List


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize(text: str) -> str:
    text = strip_accents(text.strip().lower())
    return re.sub(r"\s+", " ", text)


def tokenize_words(text: str) -> List[str]:
    return re.findall(r"[a-z0-9_]+", normalize(text))
