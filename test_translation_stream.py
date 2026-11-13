import os
import sys

import requests


URL = "https://translator-dev.felo.me/api/translation/streamV1"
TOKEN = os.getenv("FELO_TOKEN", "")

PARAMS = {
    "target_lang": "en",
    "original_lang": "zh_hans",
    "original_text": "准备好之后告诉我。",
}

HEADERS = {
    "Authorization": f"{TOKEN}",
    "Accept": "text/event-stream",
    "Cache-Control": "no-cache",
}


def main():
    if not TOKEN:
        print("错误：没有设置 FELO_TOKEN 环境变量。")
        print("请先在 PowerShell 中执行：")
        print('$env:FELO_TOKEN = "你的登录Token"')
        sys.exit(1)

    print("开始请求翻译接口...")
    print(f"原文：{PARAMS['original_text']}")
    print("-" * 60)

    try:
        with requests.get(
            URL,
            params=PARAMS,
            headers=HEADERS,
            stream=True,
            timeout=(10, 120),
        ) as response:
            print(f"HTTP 状态码：{response.status_code}")
            print(f"Content-Type：{response.headers.get('Content-Type', '')}")
            response.raise_for_status()

            received_data = []

            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue

                print(f"[SSE] {line}")

                if line.startswith("data:"):
                    data = line[5:].strip()
                    if data == "[DONE]":
                        print("收到结束标记")
                        break
                    received_data.append(data)

            print("-" * 60)
            print("流式响应接收完成")

            if received_data:
                print("data 内容：")
                print("\n".join(received_data))
            else:
                print("警告：没有收到 data 内容")
                sys.exit(2)

    except requests.exceptions.Timeout:
        print("请求超时")
        sys.exit(3)
    except requests.exceptions.HTTPError as error:
        print(f"HTTP 请求失败：{error}")
        print(f"响应内容：{response.text[:1000]}")
        sys.exit(4)
    except requests.exceptions.RequestException as error:
        print(f"请求异常：{error}")
        sys.exit(5)


if __name__ == "__main__":
    main()
