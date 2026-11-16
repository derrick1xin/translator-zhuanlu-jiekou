# translator 转录接口

用于测试 Felo 翻译流式接口（SSE），将流式片段聚合为完整译文，并可使用参考译文进行本地自动评分。

## 安装

```powershell
cd "$env:USERPROFILE\Desktop\translator转录接口"
py -m pip install -r requirements.txt
```

## 运行

在 PowerShell 中设置登录 Token（仅当前窗口有效）：

```powershell
$env:FELO_TOKEN = "你的登录Token"
```

直接运行，输出完整译文但不评分：

```powershell
py .\test_translation_stream.py
```

提供参考译文后评分：

```powershell
py .\test_translation_stream.py --reference "Tell me after you are ready."
```

也可以从 UTF-8 文件读取参考译文，避免把译文留在命令行历史中：

```powershell
py .\test_translation_stream.py --reference-file .\reference.txt
```

修改测试内容：

```powershell
py .\test_translation_stream.py `
  --original-text "准备好之后告诉我。" `
  --original-lang zh_hans `
  --target-lang en `
  --reference "Tell me after you are ready."
```

## 输出与评分

脚本会按 SSE 事件聚合 `data:` 内容，兼容纯文本和常见 JSON 文本字段，并尽量区分增量片段与累计快照。流结束后会输出完整译文；`--debug-sse` 可将原始 `data` 事件输出到 stderr。

提供参考译文时输出：

- **BLEU**：句子级词面相似度，0–100；
- **chrF**：字符级相似度，0–100；
- **综合分**：BLEU 与 chrF 各占 50%，0–100。

评分在完整译文聚合完成后进行，参考译文必须使用目标语言。分数是可复现的参考译文相似度指标，不等同于人工或语义质量评审；短句的 BLEU 可能受词数影响。未提供参考译文时会明确跳过评分。

## 认证和安全

脚本从 `FELO_TOKEN` 环境变量读取 Token，不会把 Token 写入代码。当前接口认证 Header 按实际接口协议发送 Token，不自动添加 `Bearer`。请勿将真实 Token、`.env` 文件或包含凭证的文件提交到 GitHub。
