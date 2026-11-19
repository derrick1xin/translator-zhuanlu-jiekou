"""Call the Felo translation SSE endpoint and optionally score the result."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unicodedata
from typing import Any, Iterable, Iterator, Optional

import requests


# Production endpoint. Use --url to override for a test environment.
URL = "https://translator.felo.me/api/translation/streamV1"

_TEXT_KEYS = (
    "text",
    "translation",
    "translationText",
    "translations",
    "translated_text",
    "content",
    "result",
    "output",
)
_FINAL_KEYS = ("final", "is_final", "done", "completed", "sentence_done")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="请求 Felo 翻译流式接口，并用参考译文计算 BLEU/chrF/综合分。"
    )
    # ===== 在这里填写原文（或运行时用 --original-text 覆盖） =====
    parser.add_argument(
        "--original-text",
        default='''2022年8月2日，美国国会众议长佩洛西不顾中方强烈反对和严正交涉，窜访中国台湾地区，严重违反一个中国原则和中美三个联合公报规定，严重冲击中美关系政治基础，严重侵犯中国主权和领土完整，严重破坏台海和平稳定，向“台独”分裂势力发出严重错误信号。中方对此坚决反对，严厉谴责，已向美方提出严正交涉和强烈抗议。

世界上只有一个中国，台湾是中国领土不可分割的一部分，中华人民共和国政府是代表全中国的唯一合法政府。1971年联大第2758号决议对此予以明确。1949年中华人民共和国成立以来，181个国家在一个中国原则基础上同中国建立外交关系。一个中国原则是国际社会的普遍共识和国际关系基本准则。''',  # 待翻译的原文
        help="待翻译原文。",
    )
    parser.add_argument("--original-lang", default="zh_hans", help="原文语言代码。")
    parser.add_argument("--target-lang", default="en", help="目标语言代码。")
    # ===== 在这里填写参考译文（或运行时用 --reference / --reference-file 提供） =====
    reference = parser.add_mutually_exclusive_group()
    reference.add_argument(
        "--reference",
        default="On August 2, U.S. House Speaker Nancy Pelosi made a provocative visit to China's Taiwan region despite China's strong opposition and solemn representations. The visit seriously violated the one-China principle and the provisions of the three China-U.S. joint communiqués, severely undermined the political foundation of China-U.S. relations, seriously infringed upon China's sovereignty and territorial integrity, gravely undermined peace and stability in the Taiwan Strait, and sent a seriously wrong signal to Taiwan independence separatist forces. China firmly opposes and strongly condemns this action, and has lodged solemn representations and a strong protest with the U.S.\n\nThere is only one China, and Taiwan is an inalienable part of China's territory. The government of the People's Republic of China is the sole legitimate government representing all of China. United Nations General Assembly Resolution 2758 of 1971 made this clear. Since the founding of the People's Republic of China in 1949, 181 countries have established diplomatic relations with China on the basis of the one-China principle. The one-China principle is the universal consensus of the international community and a basic norm of international relations.",  # 目标语言参考译文；与 Desktop\reference_en.txt 内容一致
        help="目标语言参考译文。",
    )
    reference.add_argument("--reference-file", help="UTF-8 参考译文文件路径。")
    parser.add_argument(
        "--debug-sse",
        action="store_true",
        help="输出原始 SSE data 事件；默认只输出聚合后的译文。",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120,
        help="读取超时秒数，默认 120。",
    )
    return parser.parse_args()


def parse_sse_events(lines: Iterable[str]) -> Iterator[str]:
    """把 SSE 的一个或多个 data 行聚合为事件 payload。"""

    data_lines: list[str] = []

    def dispatch() -> Optional[str]:
        return "\n".join(data_lines) if data_lines else None

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


def _is_final(value: Any) -> bool:
    if isinstance(value, list):
        return any(_is_final(item) for item in value)
    if not isinstance(value, dict):
        return False
    if any(_as_bool(value.get(key)) for key in _FINAL_KEYS if key in value):
        return True
    return any(
        _is_final(value[key])
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


def extract_text_chunk(payload: str) -> tuple[Optional[str], bool, str]:
    """Return text, final flag, and delta/snapshot kind from one payload."""

    if payload.strip() == "[DONE]":
        return None, True, "delta"
    try:
        parsed: Any = json.loads(payload)
    except json.JSONDecodeError:
        return (payload or None), False, "delta"
    if isinstance(parsed, str):
        return parsed or None, False, "delta"
    return _find_text(parsed), _is_final(parsed), "delta" if _contains_delta(parsed) else "snapshot"


class TextAccumulator:
    """Accumulate deltas and cumulative snapshots without duplicating snapshots."""

    def __init__(self) -> None:
        self.text = ""

    def add(self, chunk: str, kind: str) -> str:
        if not chunk:
            return ""
        if not self.text or kind == "delta":
            self.text += chunk
            return chunk
        if chunk == self.text or self.text.startswith(chunk):
            return ""
        if chunk.startswith(self.text):
            new_text = chunk[len(self.text) :]
            self.text = chunk
            return new_text
        for size in range(min(len(self.text), len(chunk)), 0, -1):
            if self.text[-size:] == chunk[:size]:
                new_text = chunk[size:]
                self.text += new_text
                return new_text
        self.text += chunk
        return chunk


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip()


def score_translation(candidate: str, reference: str) -> dict[str, float]:
    """Return sentence BLEU, chrF, and a 50/50 composite on a 0–100 scale."""

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
        raise RuntimeError(
            "评分需要 sacrebleu，请执行：py -m pip install sacrebleu"
        ) from error

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


def read_reference(args: argparse.Namespace) -> Optional[str]:
    if args.reference is not None:
        return args.reference
    if args.reference_file:
        with open(args.reference_file, "r", encoding="utf-8") as file:
            return file.read()
    return None


def main() -> int:
    args = parse_args()
    # Read credentials from the shell; never commit tokens to this repository.
    token = os.getenv("FELO_TOKEN", "")
    if not token:
        print("错误：没有设置 FELO_TOKEN 环境变量。")
        print('请先执行：$env:FELO_TOKEN = "你的登录Token"')
        return 1

    params = {
        "target_lang": args.target_lang,
        "original_lang": args.original_lang,
        "original_text": args.original_text,
    }
    headers = {
        "Authorization": token,
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }

    print("开始请求翻译接口...")
    print(f"原文：{args.original_text}")
    print("-" * 60)

    try:
        with requests.get(
            URL,
            params=params,
            headers=headers,
            stream=True,
            timeout=(10, args.timeout),
        ) as response:
            print(f"HTTP 状态码：{response.status_code}")
            print(f"Content-Type：{response.headers.get('Content-Type', '')}")
            response.raise_for_status()

            accumulator = TextAccumulator()
            done = False
            for payload in parse_sse_events(response.iter_lines(decode_unicode=True)):
                if args.debug_sse:
                    print(f"[SSE data] {payload}", file=sys.stderr)
                text, is_final, kind = extract_text_chunk(payload)
                if payload.strip() == "[DONE]":
                    done = True
                    break
                if text:
                    new_text = accumulator.add(text, kind)
                    if new_text:
                        print(new_text, end="", flush=True)
                done = done or is_final

            translation = accumulator.text
            print("\n" + "-" * 60)
            print("流式响应接收完成")
            print(f"是否收到结束标记：{'是' if done else '否'}")
            print(f"完整译文：{translation or '(空)'}")

            reference = read_reference(args)
            if reference is None:
                print("评分：跳过（未提供 --reference 或 --reference-file）")
                return

            scores = score_translation(translation, reference)
            print("评分（0–100，越高越接近参考译文）：")
            print(json.dumps(scores, ensure_ascii=False, indent=2))

    except requests.exceptions.Timeout:
        print("请求超时")
        sys.exit(3)
    except requests.exceptions.HTTPError as error:
        body = error.response.text[:1000] if error.response is not None else ""
        print(f"HTTP 请求失败：{error}")
        print(f"响应内容：{body}")
        sys.exit(4)
    except requests.exceptions.RequestException as error:
        print(f"请求异常：{error}")
        sys.exit(5)
    except OSError as error:
        print(f"参考译文文件读取失败：{error}")
        sys.exit(6)
    except RuntimeError as error:
        print(f"评分失败：{error}")
        sys.exit(7)


if __name__ == "__main__":
    raise SystemExit(main())
