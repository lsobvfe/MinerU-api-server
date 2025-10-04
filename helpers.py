# helpers.py
import requests
import os
import json
import glob
import time
import zipfile
import shutil
from io import BytesIO
from queue_manager import STATUS_DONE, STATUS_FAILED

# --- 配置参数 ---
TOKEN = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFM1MTIifQ.eyJqdGkiOiI4NjkwMDg2OCIsInJvbCI6IlJPTEVfUkVHSVNURVIiLCJpc3MiOiJPcGVuWExhYiIsImlhdCI6MTc1OTIxODY4MSwiY2xpZW50SWQiOiJsa3pkeDU3bnZ5MjJqa3BxOXgydyIsInBob25lIjoiIiwib3BlbklkIjpudWxsLCJ1dWlkIjoiNTEzN2U0OWUtZGI2Ni00MmM1LTgzNzgtMjFmY2Q4MDAxNzFhIiwiZW1haWwiOiIiLCJleHAiOjE3NjA0MjgyODF9.sldwNqDCX830TwrVeHW_dQZ_Kid7Z8yrRwwV7SoqtcVSZdyJfCRl0mdWga1hrHjhUzv1G-s6A3FKSCaooefalw"
NUM_FILES_TO_UPLOAD = 50 # 每次循环处理的文件数量
URL_APPLY_URLS = "https://mineru.net/api/v4/file-urls/batch"
URL_QUERY_RESULT_BASE = "https://mineru.net/api/v4/extract-results/batch"
MAX_CHECKS = 100
INTERVAL = 15

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

# --- 核心函数 ---

def find_pdf_files(directory, count):
    """查找目录下的前 N 个 PDF 文件"""
    pdf_paths = glob.glob(os.path.join(directory, "*.pdf"))
    
    selected_paths = pdf_paths[:count]
    file_details = []
    for path in selected_paths:
        file_details.append({
            "name": os.path.basename(path),
            "path": path,
        })
    return file_details

def apply_upload_urls(file_list):
    """申请上传链接"""
    print("\n--- 1. 批量申请上传链接 ---")
    
    api_files = []
    for i, f in enumerate(file_list):
        api_files.append({
            "name": f["name"],
            "is_ocr": True,
            "dat-id": f"demo_file_{i+1}",
            "model_version": "vlm"
        })
    
    DATA = {
        "enable_formula": True,
        "language": "ch",
        "enable_table": False,
        "files": api_files
    }

    try:
        response = requests.post(URL_APPLY_URLS, headers=HEADERS, json=DATA)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == 0:
            batch_id = result["data"]["batch_id"]
            file_urls = result["data"]["file_urls"]
            
            if len(file_urls) != len(file_list):
                 raise Exception(f"请求成功，但返回的链接数量 ({len(file_urls)}) 与文件数量 ({len(file_list)}) 不匹配。")
            
            for i in range(len(file_list)):
                file_list[i]['upload_url'] = file_urls[i]

            print(f"成功获取上传链接。Batch ID: {batch_id}")
            return batch_id, file_list
        else:
            print(f"申请上传链接失败。错误信息: {result.get('msg')}")
            return None, None
            
    except requests.exceptions.RequestException as e:
        print(f"申请上传链接请求失败: {e}")
        return None, None
    except Exception as e:
        print(f"处理响应时发生错误: {e}")
        return None, None

def upload_files(file_list):
    """上传所有文件"""
    print("\n--- 2. 开始上传文件 ---")
    success_count = 0
    
    for f in file_list:
        file_path = f["path"]
        upload_url = f["upload_url"]
        
        try:
            with open(file_path, 'rb') as data:
                res_upload = requests.put(upload_url, data=data)
                res_upload.raise_for_status()
                
                print(f"  ✅ '{f['name']}' 上传成功！")
                success_count += 1
        except requests.exceptions.RequestException as e:
            print(f"  ❌ '{f['name']}' 上传失败。错误: {e}")
            
    return success_count == len(file_list)

def poll_for_results(batch_id):
    """轮询任务状态，直到完成"""
    print(f"\n--- 3. 开始轮询任务状态 (Batch ID: {batch_id}) ---")
    query_url = f"{URL_QUERY_RESULT_BASE}/{batch_id}"
    
    for i in range(1, MAX_CHECKS + 1):
        print(f"\n[轮询检查 {i}/{MAX_CHECKS}] 正在查询...")
        try:
            response = requests.get(query_url, headers=HEADERS)
            response.raise_for_status()
            result = response.json()

            if result.get("code") != 0:
                print(f"查询失败。错误信息: {result.get('msg', '未知错误')}")
                return None
            
            extract_results = result["data"]["extract_result"]
            all_finished = True
            
            for file_result in extract_results:
                state = file_result.get("state")
                file_name = file_result.get("file_name", "N/A")
                
                if state in ["running", "pending", "waiting-file"]:
                    all_finished = False
                    if state == "running" and "extract_progress" in file_result:
                        progress = file_result["extract_progress"]
                        print(f"  - {file_name}: {state}. 进度: {progress.get('extracted_pages', 0)}/{progress.get('total_pages', 0)} 页")
                    else:
                        print(f"  - {file_name}: {state}...")
                
                elif state == "failed":
                    print(f"  - {file_name}: ❌ 任务失败。原因: {file_result.get('err_msg', '未知')}")
                
                elif state == "done":
                    print(f"  - {file_name}: ✅ 完成。")
            
            if all_finished:
                print("\n--- 所有任务处理结束 ---")
                return extract_results

        except requests.exceptions.RequestException as e:
            print(f"查询请求发生异常: {e}")
            return None
        
        time.sleep(INTERVAL)

    print("\n--- 达到最大轮询次数，任务可能仍在处理中 ---")
    return None

def download_and_extract(results, processed_dir, markdown_dir):
    """下载并解压所有完成任务的结果，并将 full.md 复制到 markdown 目录"""
    print("\n--- 4. 下载、解压并处理结果文件 ---")
    
    for res in results:
        file_name_pdf = res.get("file_name")
        state = res.get("state")
        
        if state != "done":
            print(f"跳过文件 '{file_name_pdf}'，状态为 '{state}'。")
            continue
            
        zip_url = res.get("full_zip_url")
        if not zip_url:
            print(f"文件 '{file_name_pdf}' 状态为 done，但未找到 zip URL。")
            continue
            
        print(f"\n开始处理 '{file_name_pdf}' 的结果...")
        
        # 1. 定义解压路径
        # 使用不带后缀的文件名作为子目录名
        base_file_name = os.path.splitext(file_name_pdf)[0]
        extract_path = os.path.join(processed_dir, base_file_name + "_result")
        os.makedirs(extract_path, exist_ok=True)
        
        try:
            # 2. 下载 ZIP 包
            zip_response = requests.get(zip_url, stream=True)
            zip_response.raise_for_status()
            
            # 3. 解压
            with BytesIO(zip_response.content) as zip_buffer:
                with zipfile.ZipFile(zip_buffer, 'r') as zf:
                    zf.extractall(extract_path)
                    print(f"  ✅ 结果下载并成功解压到: {extract_path}")
            
            # 4. 查找并复制 full.md
            md_source_path = os.path.join(extract_path, "full.md")
            
            if os.path.exists(md_source_path):
                # 新的 Markdown 文件名：原 PDF 文件名 (去除 .pdf) + .md
                md_target_name = base_file_name + ".md"
                md_target_path = os.path.join(markdown_dir, md_target_name)
                
                # 复制文件
                shutil.copy2(md_source_path, md_target_path)
                print(f"  ✅ full.md 已复制并命名为 '{md_target_name}' 到 '{markdown_dir}'")
            else:
                print(f"  ⚠️ 未在解压目录中找到 full.md 文件。")

        except requests.exceptions.RequestException as e:
            print(f"  ❌ 下载或请求文件 '{file_name_pdf}' 失败: {e}")
        except zipfile.BadZipFile:
            print(f"  ❌ 下载的文件 '{file_name_pdf}' 不是有效的 ZIP 文件。")
        except Exception as e:
            print(f"  ❌ 文件处理过程中发生错误: {e}")