# app.py
from flask import Flask, request, jsonify
from queue_manager import get_queue, STATUS_UNDONE
import os
from flask_restx import Api, Resource, fields # 导入 Api, Resource 和 fields

app = Flask(__name__)
QUEUE = get_queue()

api = Api(app, version='1.0', title='MinerU OCR API', description='A simple API for submitting OCR tasks')

ns = api.namespace('ocr', description='OCR operations')

task_model = api.model('Task', {
    'directory_path': fields.String(required=True, description='The path to the directory containing PDF files')
})

@ns.route('/submit-ocr-task') # 将路由注册到命名空间
class SubmitOcrTask(Resource):
    @ns.expect(task_model)
    @ns.doc(description='接收目录地址并加入任务队列')
    def post(self):
        """接收目录地址并加入任务队列"""
        data = request.get_json()
        directory_path = data.get('directory_path')

        if not directory_path:
            return {"code": 400, "msg": "Missing 'directory_path'"}, 400

        # 简单验证路径是否存在
        if not os.path.isdir(directory_path):
            return {"code": 400, "msg": f"Directory not found or is not a directory: {directory_path}"}, 400

        try:
            task_data = {
                "directory_path": directory_path,
                "status": STATUS_UNDONE  # 将状态存储在 data 字典中
            }
            QUEUE.send_message(task_data) # 提交整个字典
            
            # 打印队列当前大小 (大致)
            size = QUEUE.qsize()

            return {
                "code": 200, 
                "msg": "Task submitted successfully",
                "directory": directory_path,
                "queue_size": size + 1
            }
        except Exception as e:
            return {"code": 500, "msg": f"Failed to add task to queue: {e}"}, 500

@app.route('/swagger.json')
def swagger_json():
    return jsonify(api.__schema__)

if __name__ == '__main__':
    # 注意：在生产环境中，应使用 WSGI 服务器 (如 Gunicorn) 运行
    app.run(host='0.0.0.0', port=5001)