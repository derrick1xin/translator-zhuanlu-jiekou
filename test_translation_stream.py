"""Test the translation stream API and score its complete result."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import requests

from translation_stream import (
    SentenceEmitter,
    TextAccumulator,
    extract_text_chunk,
    is_stream_done,
    parse_sse_events,
    score_translation,
)


URL = "https://translator-dev.felo.me/api/translation/streamV1"
DEFAULT_ORIGINAL_TEXT = "准备好之后告诉我。"
DEFAULT_TARGET_LANG = "en"
DEFAULT_ORIGINAL_LANG = "zh_hans"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="读取翻译 SSE 流，输出完整译文并用参考译文自动评分。"
    )
    parser.add_argument("--url", default=URL, help="翻译接口地址")
    parser.add_argument("--original-text", default=DEFAULT_ORIGINAL_TEXT, help="待翻译原文")
    parser.add_argument("--original-lang", default=DEFAULT_ORIGINAL_LANG, help="原文语言")
    parser.add_argument("--target-lang", default=DEFAULT_TARGET_LANG, help="目标语言")
    reference = parser.add_mutually_exclusive_group()
    reference.add_argument("--reference", help="参考译文文本")
    reference.add_argument("--reference-file", type=Path, help="UTF-8 编码的参考译文文件")
    parser.add_argument(
        "--debug-sse",
        action="store_true",
        help="将原始 SSE data 事件输出到 stderr，便于排查接口字段",
    )
    return parser


def load_reference(args: argparse.Namespace) -> Optional[str]:
    if args.reference is not None:
        reference = args.reference
    elif args.reference_file is not None:
        try:
            reference = args.reference_file.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"无法读取参考译文文件：{error}") from error
    else:
        return None

    if not reference.strip():
        raise ValueError("参考译文不能为空")
    return reference


def run(args: argparse.Namespace) -> int:
    token = os.getenv("FELO_TOKEN", "")
    if not token:
        print("错误：没有设置 FELO_TOKEN 环境变量。")
        print("请先在 PowerShell 中执行：")
        print('$env:FELO_TOKEN = "你的登录Token"')
        return 1

    try:
        reference = load_reference(args)
    except ValueError as error:
        print(f"参数错误：{error}")
        return 1

    params = {
        "target_lang": args.target_lang,
        "original_lang": args.original_lang,
        "original_text": args.original_text,
    }
    # 保持接口当前实际认证协议：不自动添加 Bearer。
    headers = {
        "Authorization": f"{token}",
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }

    print("开始请求翻译接口...")
    print(f"原文：{args.original_text}")
    print("-" * 60)

    accumulator = TextAccumulator()
    emitter = SentenceEmitter()
    response = None
    try:
        response = requests.get(
            args.url,
            params=params,
            headers=headers,
            stream=True,
            timeout=(10, 120),
        )
        print(f"HTTP 状态码：{response.status_code}")
        print(f"Content-Type：{response.headers.get('Content-Type', '')}")
        response.raise_for_status()

        for payload in parse_sse_events(response.iter_lines(decode_unicode=True)):
            if args.debug_sse:
                print(f"[SSE data] {payload}", file=sys.stderr)
            if is_stream_done(payload):
                break

            chunk = extract_text_chunk(payload)
            if chunk is None:
                continue
            added = accumulator.add(chunk.text, kind=chunk.kind)
            for sentence in emitter.feed(added, final=chunk.is_final):
                print(f"[完整句子] {sentence}")

    except requests.exceptions.Timeout:
        print("请求超时")
        return 3
    except requests.exceptions.HTTPError as error:
        print(f"HTTP 请求失败：{error}")
        if response is not None:
            print(f"响应内容：{response.text[:1000]}")
        return 4
    except requests.exceptions.RequestException as error:
        print(f"请求异常：{error}")
        return 5

    for sentence in emitter.feed("", final=True):
        print(f"[完整句子] {sentence}")

    print("-" * 60)
    print("最终完整译文：")
    print(accumulator.text or "（空）")

    if reference is None:
        print("未提供参考译文，跳过自动评分。")
    else:
        try:
            scores = score_translation(accumulator.text, reference)
        except (RuntimeError, ValueError) as error:
            print(f"评分失败：{error}")
            return 2
        print("参考译文自动评分（本地指标，非人工语义评审）：")
        print(f"  BLEU：{scores['bleu']:.2f}/100")
        print(f"  chrF：{scores['chrf']:.2f}/100")
        print(f"  综合分（BLEU 50% + chrF 50%）：{scores['composite']:.2f}/100")

    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
