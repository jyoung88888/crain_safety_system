import psycopg2
import os
from config.config import Config
# 1.이미지 로드 및 기본 유틸리티
# db 관련 함수
def get_connection(context=""):
    """
    Returns a new PostgreSQL connection.
    Manages connection errors and prints appropriate error messages.

    Args:
        context (str): 호출 컨텍스트 정보 (예: "[CAM0005] get_camera_by_id")
    """
    tag = f" {context}" if context else ""
    try:
        # 환경변수가 있으면 우선 사용, 없으면 config.py의 기본값 사용
        host = os.getenv("DB_HOST", Config.DB_HOST)
        port = os.getenv("DB_PORT", str(Config.DB_PORT))
        database = os.getenv("DB_DATABASE", Config.DB_DATABASE)
        user = os.getenv("DB_USER", Config.DB_USER)
        password = os.getenv("DB_PASSWORD", Config.DB_PASSWORD)

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        print(f"[DB Connected]{tag}")
        return conn
    except Exception as e:
        print(f"[DB Connect Failed]{tag}: {e}")
        return None