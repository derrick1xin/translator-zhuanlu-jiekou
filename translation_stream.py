"""Helpers for parsing translation SSE streams and scoring translations."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional


@dataclass(frozen=True)
class TextChunk:
    """Text extracted from one SSE event."""

    text: str
    is_final: bool = False
    # ``delta`` is newly generated text; ``snapshot`` is cumulative text.
    kind: str = "delta"


_TEXT_KEYS = (
    "text",
    "translation",
    "translations",
    "translated_text",
    "content",
    "result",
    "output",
)
_FINAL_KEYS = ("final", "is_final", "done", "completed", "sentence_done")
_CLOSING_QUOTES = "\"'”’」』〉》」)]}"


def parse_sse_events(lines: Iterable[str]) -> Iterator[str]:
    """Yield one combined ``data:`` payload for each SSE event.

    SSE permits multiple data lines in one event. The protocol joins them with
    a newline when the blank separator is received. A final unterminated event
    is also dispatched, which is useful for test doubles and imperfect servers.
    """

    data_lines: list[str] = []

    def dispatch() -> Optional[str]:
        if not data_lines:
            return None
        return "\n".join(data_lines)

    for raw_line in lines:
        line = raw_line.rstrip("\r\n")
        if not line:
            payload = dispatch()
            data_lines.clear()
            if payload is not None:
                yield payload
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if field != "data":
            continue
        if separator and value.startswith(" "):
            value = value[1:]
        data_lines.append(value)

    payload = dispatch()
    if payload is not None:
        yield payload


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "done", "final"}
    return bool(value)


def _final_flag(value: Any) -> bool:
    if isinstance(value, list):
        return any(_final_flag(item) for item in value)
    if not isinstance(value, dict):
        return False
    if any(_as_bool(value.get(key)) for key in _FINAL_KEYS if key in value):
        return True
    return any(
        _final_flag(value[key])
        for key in ("delta", "message", "data", "result", "output", "choices")
        if key in value
    )


def _contains_delta(value: Any) -> bool:
    if isinstance(value, list):
        return any(_contains_delta(item) for item in value)
    if not isinstance(value, dict):
        return False
    if "delta" in value:
        return True
    return any(
        _contains_delta(value[key])
        for key in ("message", "data", "result", "output", "choices")
        if key in value
    )


def _find_text(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for item in value:
            text = _find_text(item)
            if text is not None:
                return text
        return None
    if not isinstance(value, dict):
        return None

    # OpenAI-style streams and common nested API response shapes.
    for key in ("delta", "message", "data", "result", "output", "choices"):
        if key in value:
            text = _find_text(value[key])
            if text is not None:
                return text
    for key in _TEXT_KEYS:
        item = value.get(key)
        if isinstance(item, str):
            return item
        if isinstance(item, (dict, list)):
            text = _find_text(item)
            if text is not None:
                return text
    return None


def is_stream_done(payload: str) -> bool:
    return payload.strip() == "[DONE]"


def extract_text_chunk(payload: str) -> Optional[TextChunk]:
    """Extract a translation fragment from a plain-text or JSON SSE payload."""

    if is_stream_done(payload):
        return None

    try:
        parsed: Any = json.loads(payload)
    except json.JSONDecodeError:
        text = payload
        return TextChunk(text=text, is_final=False, kind="delta") if text else None

    if isinstance(parsed, str):
        return TextChunk(parsed, kind="delta")
    text = _find_text(parsed)
    if text is None:
        return None
    is_final = _final_flag(parsed)
    kind = "delta" if _contains_delta(parsed) else "snapshot"
    return TextChunk(text=text, is_final=is_final, kind=kind)


class TextAccumulator:
    """Accumulate delta chunks and cumulative snapshots without duplication."""

    def __init__(self) -> None:
        self.text = ""

    def add(self, chunk: str, kind: str = "delta") -> str:
        """Add a chunk and return only the newly added text.

        Explicit deltas are never globally de-duplicated: repeated words can
        be valid translation output. Snapshot payloads are reconciled against
        the accumulated prefix to avoid ``HelloHello world``.
        """

        if not chunk:
            return ""
        if not self.text or kind == "delta":
            self.text += chunk
            return chunk

        previous = self.text
        if chunk == previous or previous.startswith(chunk):
            return ""
        if chunk.startswith(previous):
            self.text = chunk
            return chunk[len(previous) :]

        # Handle a cumulative snapshot whose first characters overlap the
        # current suffix. This is intentionally local, not global de-duplication.
        max_overlap = min(len(previous), len(chunk))
        for size in range(max_overlap, 0, -1):
            if previous[-size:] == chunk[:size]:
                self.text += chunk[size:]
                return chunk[size:]
        self.text += chunk
        return chunk


class SentenceEmitter:
    """Emit complete sentences from a stream of newly added text."""

    def __init__(self) -> None:
        self.pending = ""

    def feed(self, text: str, final: bool = False) -> list[str]:
        self.pending += text
        sentences: list[str] = []
        start = 0
        index = 0
        while index < len(self.pending):
            char = self.pending[index]
            boundary = char in "。！？；!?"
            if char == ".":
                next_char = self.pending[index + 1] if index + 1 < len(self.pending) else ""
                boundary = not next_char or next_char.isspace()
            if boundary:
                end = index + 1
                while end < len(self.pending) and self.pending[end] in _CLOSING_QUOTES:
                    end += 1
                sentence = self.pending[start:end].strip()
                if sentence:
                    sentences.append(sentence)
                start = end
                index = end
                continue
            index += 1

        self.pending = self.pending[start:]
        if final and self.pending.strip():
            sentences.append(self.pending.strip())
            self.pending = ""
        return sentences


def normalize_text(text: str) -> str:
    """Normalize Unicode and whitespace before comparing translations."""

    normalized = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", normalized).strip()


def score_translation(candidate: str, reference: str) -> dict[str, float]:
    """Return BLEU, chrF, and their 50/50 composite score on a 0-100 scale.

    chrF is used in its standard character-only form (word_order=0), not chrF++.
    """

    candidate = normalize_text(candidate)
    reference = normalize_text(reference)
    if not reference:
        raise ValueError("参考译文不能为空")
    if not candidate:
        return {"bleu": 0.0, "chrf": 0.0, "composite": 0.0}
    if candidate == reference:
        return {"bleu": 100.0, "chrf": 100.0, "composite": 100.0}

    try:
        from sacrebleu import sentence_bleu, sentence_chrf
    except ImportError as error:
        raise RuntimeError("评分需要 sacrebleu，请先执行：py -m pip install -r requirements.txt") from error

    bleu = sentence_bleu(
        candidate,
        [reference],
        smooth_method="exp",
        use_effective_order=True,
    ).score
    chrf = sentence_chrf(
        candidate,
        [reference],
        char_order=6,
        word_order=0,
        beta=2,
    ).score
    bleu = max(0.0, min(100.0, float(bleu)))
    chrf = max(0.0, min(100.0, float(chrf)))
    return {"bleu": bleu, "chrf": chrf, "composite": (bleu + chrf) / 2}
