# translator 转录接口

用于测试 Felo 翻译流式接口（SSE）。

## 使用方法

1. 安装依赖：

   ```powershell
   py -m pip install requests
   ```

2. 设置登录 Token（仅当前 PowerShell 窗口有效）：

   ```powershell
   $env:FELO_TOKEN = "你的登录Token"
   ```

3. 运行：

   ```powershell
   py .\test_translation_stream.py
   ```

脚本不会保存或包含真实登录 Token。请求地址、语言和原文可以在 Python 文件顶部修改。
