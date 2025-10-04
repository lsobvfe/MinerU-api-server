# Mineru OCR Processing Service

本项目提供了一个基于 Flask 和自定义 SQLite 持久化队列的异步 OCR 处理服务。它能够接收目录路径，将目录中的 PDF 文件分批次上传、解析，并将最终结果（Markdown 和原始解压文件）整理到指定的子目录中。

## 项目结构
/mineru-ocr-service
|-- app.py # Flask Service (API 接口)
|-- worker.py # Worker (后台处理逻辑)
|-- helpers.py # 核心 API 交互和文件处理函数
|-- queue_manager.py # 队列管理 (自定义 SQLite 实现，包含 `undone`, `running`, `done`, `failed` 状态)
|-- readme.md
|-- requirements.txt
code
Code
## 运行环境配置

1. **安装依赖:**
   ```bash
   pip install -r requirements.txt
   ```

2. **配置 Token:**
   在 `helpers.py` 或系统环境变量中设置 `MINERU_TOKEN`。

3. **配置每次上传的文件数量:**
   在 `helpers.py` 中修改 `NUM_FILES_TO_UPLOAD` 变量，控制每次处理的 PDF 文件批次大小 (默认为 5)。

## 如何使用

本项目分为两个独立运行的部分：API 服务和 Worker 服务。

1. **启动 API 服务 (`app.py`)**

   ```bash
   python app.py
   ```

   默认运行在 `http://127.0.0.1:5001`。

2. **启动 Worker 服务 (`worker.py`)**

   Worker 会持续监听队列，处理任务。

   ```bash
   python worker.py
   ```

3. **提交任务**

   使用 CURL 或 Postman 向 API 提交需要处理的目录路径。

   接口: `POST /submit-ocr-task`

   示例 CURL 请求:

   ```bash
   curl -X POST http://127.0.0.1:5001/submit-ocr-task \
   -H "Content-Type: application/json" \
   -d '{
       "directory_path": "D:\\Desktop\\ocr\\裴礼文数分" 
   }'
   ```

   提交成功后，Worker 会自动开始处理该目录下的所有 PDF 文件。

## 任务处理逻辑

Worker 遵循以下流程，并以批次循环方式处理目录中的所有 PDF 文件：

1. **查找文件**: 从目录中查找 `NUM_FILES_TO_UPLOAD` (默认 5) 个未处理的 PDF 文件。
2. **API 交互**: 申请上传链接，并执行文件上传。
3. **轮询**: 持续查询 `batch_id` 状态，直到所有文件解析完成 (done 或 failed)。
4. **下载/解压**: 下载结果 ZIP 包。
   解压到 `[目录地址]/processed/[原文件名]_result/`。
5. **文件整理**:
   将解压目录中的 `full.md` 文件复制到 `[目录地址]/markdown`
6. **文件清理**: 将已成功处理的原始 PDF 文件移动到 `[目录地址]/done` 子目录。

## OpenAPI 规范

您可以通过以下 URL 访问 OpenAPI 规范 (JSON 格式):

```
http://127.0.0.1:5001/swagger.json
```

您也可以通过浏览器访问 Swagger UI (如果已配置):

```
http://127.0.0.1:5001/swagger
```