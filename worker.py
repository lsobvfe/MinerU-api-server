# worker.py
import os
import time
import glob # 添加 glob 导入
from queue_manager import get_queue, STATUS_DONE, STATUS_FAILED, STATUS_RUNNING, STATUS_UNDONE
from helpers import (
    find_pdf_files, apply_upload_urls, upload_files, 
    poll_for_results, download_and_extract,
    TOKEN, NUM_FILES_TO_UPLOAD # 确保 TOKEN 和 NUM_FILES_TO_UPLOAD 被导入
)
import shutil # 添加 shutil 导入

# 确保 TOKEN 环境变量设置
if not os.environ.get("MINERU_TOKEN"):
    # 临时设置，实际部署时应使用 .env 或系统环境变量
    os.environ["MINERU_TOKEN"] = TOKEN 
    
QUEUE = get_queue()

def process_directory(task_data, task_id):
    """
    处理一个目录任务，直到该目录下所有 PDF 文件处理完毕。
    """
    base_dir = task_data.get("directory_path") 

    print(f"\n=======================================================")
    print(f"🔥 WORKER START: Processing Task ID {task_id} in Dir: {base_dir}")
    print(f"=======================================================")

    processed_count = 0
    # 这里应该计算尚未处理的 PDF 文件总数，而不是目录中的所有文件
    # 为了简化，我们暂时保留这个计数方式，但需注意其可能不完全准确
    total_files_in_dir = len(glob.glob(os.path.join(base_dir, "*.pdf")))
    
    # 定义结果子目录
    processed_dir_path = os.path.join(base_dir, "processed")
    markdown_dir_path = os.path.join(base_dir, "markdown")
    done_dir_path = os.path.join(base_dir, "done") # 用于存放已处理的原始 PDF

    os.makedirs(processed_dir_path, exist_ok=True)
    os.makedirs(markdown_dir_path, exist_ok=True)
    os.makedirs(done_dir_path, exist_ok=True)
    
    # 目录级别循环：只要目录中还有未处理的文件，就继续循环
    while True:
        # 1. 查找待处理的文件
        # 改进：find_pdf_files 应该只返回未在 'done' 目录中的文件
        current_pdf_files = [f for f in glob.glob(os.path.join(base_dir, "*.pdf")) if not os.path.exists(os.path.join(done_dir_path, os.path.basename(f)))]
        
        file_list = find_pdf_files(base_dir, NUM_FILES_TO_UPLOAD)
        # 进一步过滤，确保只处理那些不在 done_dir_path 中的文件
        file_list = [f for f in file_list if not os.path.exists(os.path.join(done_dir_path, f["name"]))]
        
        if not file_list:
            print(f"\n✅ 目录 '{base_dir}' 中的所有文件已处理完毕 (共处理 {processed_count} 个文件). ")
            return True # 任务成功完成

        print(f"\n--- Found {len(file_list)} files for current batch ({len(current_pdf_files) - len(file_list)} remaining in directory for next batch) ---")

        # 2. 申请上传链接
        batch_id, file_list_with_urls = apply_upload_urls(file_list)
        if not batch_id:
            print("Batch ID 获取失败，跳过当前批次...")
            return False # 标记任务失败

        # 3. 上传文件
        if not upload_files(file_list_with_urls):
            print("文件上传失败，跳过当前批次...")
            return False

        # 4. 轮询结果
        final_results = poll_for_results(batch_id)

        if final_results:
            # 5. 下载、解压和处理文件
            download_and_extract(final_results, processed_dir_path, markdown_dir_path)
            
            # 6. 清理文件：将原始 PDF 文件移动到 done 子目录
            successful_files_in_batch = []
            for res in final_results:
                if res.get("state") == "done":
                    # 从原始 file_list 中找到对应的文件路径
                    original_file_name = res.get("file_name")
                    for f_detail in file_list:
                        if f_detail["name"] == original_file_name:
                            successful_files_in_batch.append(f_detail["path"])
                            break
            
            for path in successful_files_in_batch:
                try:
                    shutil.move(path, os.path.join(done_dir_path, os.path.basename(path)))
                    processed_count += 1
                    print(f"  ✅ 已将 '{os.path.basename(path)}' 移动到 'done' 目录。")
                except Exception as e:
                    print(f"  ❌ 无法移动文件 {path}: {e}")
            
            print(f"--- 批次处理完成。成功处理 {len(successful_files_in_batch)} 个文件。---")

        else:
            print("轮询结果失败，跳过当前批次...")
            return False # 标记任务失败

        # 小憩一下，避免连续高频操作
        time.sleep(2) 

    return False # 任务失败退出，理论上在上面的 return 处已经退出
    
def worker_loop():
    """Worker主循环，不断从队列中取任务"""
    print("--- Mineru OCR Worker 启动 ---")
    while True:
        try:
            task_item = QUEUE.get_message() # 使用 get_message 获取任务

            if task_item:
                task_id = task_item['id']
                task_data = task_item['data']
                
                success = process_directory(task_data, task_id)
                
                # 根据结果更新最终状态
                if success:
                    QUEUE.complete_message(task_id)
                    print(f"✅ Task ID {task_id} 完成。")
                else:
                    QUEUE.fail_message(task_id)
                    print(f"❌ Task ID {task_id} 失败。")
            else:
                print("等待任务... (5s)")
                time.sleep(5) # 没有任务时等待一段时间

        except Exception as e:
            print(f"Worker 发生致命错误: {e}")
            time.sleep(10) # 避免错误循环

if __name__ == '__main__':
    worker_loop()