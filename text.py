"""文本规范化与字符级 shingle（从 simhash.py 抽出来，去重链路唯一依赖的部分）。

simhash / minhash / 精确预聚合都走这一套，保证「同一批字节」：
  normalize   全角->半角、小写、去标点、空白压成单空格
  shingles    字符级 n-gram（中文场景比切词更轻、无词典依赖）
  ws_key      去所有空白后的字符串（精确重复判等用：本语料的 exact 副本只差换行）
"""
from __future__ import annotations

import re
import unicodedata

_WS = re.compile(r"\s+")


def normalize(text: str) -> str:
    """全角->半角、去标点空白、小写，中文按字保留。"""
    text = unicodedata.normalize("NFKC", text).lower()
    text = re.sub(r"[\s\u3000]+", " ", text)
    text = re.sub(r"[^\w\u4e00-\u9fff ]+", "", text)
    return text.strip()


def shingles(text: str, n: int = 3) -> list[str]:
    """字符级 n-gram。"""
    t = normalize(text).replace(" ", "")
    if len(t) <= n:
        return [t] if t else []
    return [t[i: i + n] for i in range(len(t) - n + 1)]


def ws_key(text: str) -> str:
    """只去掉所有空白（不规范化）——精确重复的判等键。"""
    return _WS.sub("", text)
