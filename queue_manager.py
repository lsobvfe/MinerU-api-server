# queue_manager.py
import sqlite3
import json
import os

# 数据库文件路径
DB_PATH = 'task_queue.db'
QUEUE_NAME = 'ocr_tasks' # 队列/表名

# 状态枚举
STATUS_UNDONE = 'undone'
STATUS_RUNNING = 'running'
STATUS_DONE = 'done'
STATUS_FAILED = 'failed'

class SqliteQueue:
    def __init__(self, db_path, queue_name):
        self.db_path = db_path
        self.queue_name = queue_name
        self._create_table_if_not_exists()

    def _get_db_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 允许通过列名访问数据
        return conn

    def _create_table_if_not_exists(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.queue_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT '{STATUS_UNDONE}'
            )
        """)
        conn.commit()
        conn.close()

    def send_message(self, task_data):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {self.queue_name} (data, status) VALUES (?, ?)
        """, (json.dumps(task_data), STATUS_UNDONE))
        conn.commit()
        conn.close()

    def get_message(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT id, data FROM {self.queue_name}
            WHERE status = '{STATUS_UNDONE}'
            ORDER BY id ASC
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        if row:
            task_id = row['id']
            task_data = json.loads(row['data'])
            cursor.execute(f"""
                UPDATE {self.queue_name} SET status = '{STATUS_RUNNING}' WHERE id = ?
            """, (task_id,))
            conn.commit()
            conn.close()
            return {'id': task_id, 'data': task_data}
        conn.close()
        return None

    def complete_message(self, task_id):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE {self.queue_name} SET status = '{STATUS_DONE}' WHERE id = ?
        """, (task_id,))
        conn.commit()
        conn.close()

    def fail_message(self, task_id):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            UPDATE {self.queue_name} SET status = '{STATUS_FAILED}' WHERE id = ?
        """, (task_id,))
        conn.commit()
        conn.close()

    def qsize(self):
        conn = self._get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) FROM {self.queue_name}
            WHERE status = '{STATUS_UNDONE}'
        """)
        count = cursor.fetchone()[0]
        conn.close()
        return count

def get_queue():
    """初始化并返回任务队列"""
    queue = SqliteQueue(DB_PATH, QUEUE_NAME)
    return queue

# 初始化队列（可以在启动时调用一次）
if __name__ == '__main__':
    current_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(current_dir, DB_PATH)
    
    q = get_queue()
    print(f"任务队列 '{QUEUE_NAME}' 已准备好在 {db_path}")