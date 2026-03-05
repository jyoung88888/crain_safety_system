import os
import ast
import zmq
import json
import base64
import cv2
import queue
import time
import psycopg2
from psycopg2 import OperationalError


def to_jsonable(obj):
    """numpy/torch 객체를 JSON 직렬화 가능 타입으로 변환."""
    try:
        import numpy as np
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    except ImportError:
        pass
    try:
        import torch
        if isinstance(obj, torch.Tensor):
            return obj.detach().cpu().tolist()
    except ImportError:
        pass
    return obj

current_directory = os.getcwd()

image_save_dir = current_directory + "/img/event/"

def server(image_queue, grid_st_queue, port):

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{port}")

    print("Server started, waiting for client requests...", flush=True)

    # CCTV별 최신값 캐시
    last_image_map = {}     # {cctv_id: image}
    last_grid_map = {}      # {cctv_id: grid_st}

    try:
        while True:
            raw = socket.recv_string()
            print(f"Received request: {raw}", flush=True)

            # 1) JSON 요청 지원 (기존 문자열 요청도 호환)
            try:
                req = json.loads(raw)
                cmd = req.get("message") or req.get("cmd")
                cctv_id = req.get("cctv_id") or req.get("out_path") or req.get("pid")
            except Exception:
                cmd = raw
                cctv_id = None

            # 2) 큐에서 가능한 만큼 뽑아서 캐시 최신화 (요청 시점에 최신 상태 반영)
            #    - 큐가 커지는 것 방지 + 여러 CCTV 데이터 섞여 들어와도 문제 없음
            try:
                while True:
                    cid, img = image_queue.get_nowait()
                    last_image_map[cid] = img
            except queue.Empty:
                pass

            try:
                while True:
                    cid, st = grid_st_queue.get_nowait()
                    last_grid_map[cid] = st
            except queue.Empty:
                pass

            # 3) 요청 처리
            if cmd == "send_image":
                if not cctv_id:
                    socket.send_string("Error: cctv_id required")
                    continue

                image = last_image_map.get(cctv_id)
                if image is None:
                    socket.send_string("Error: No image for cctv_id")
                    continue

                ok, buffer = cv2.imencode(".jpg", image)  # ✅ 확장자 .jpg 로
                if not ok:
                    socket.send_string("Error: encode failed")
                    continue

                socket.send_string(base64.b64encode(buffer).decode("utf-8"))
                print(f"Image sent. cctv_id={cctv_id}", flush=True)

            elif cmd == "send_grid_st":
                if not cctv_id:
                    socket.send_string("Error: cctv_id required")
                    continue

                grid_st = last_grid_map.get(cctv_id)
                if grid_st is None:
                    socket.send_string("Error: No grid_st for cctv_id")
                    continue

                socket.send_string(json.dumps(grid_st, ensure_ascii=False, default=to_jsonable))
                print(f"grid_st sent. cctv_id={cctv_id}", flush=True)

            else:
                socket.send_string("Unknown request")

    except KeyboardInterrupt:
        print("Server shutting down...")

    finally:
        socket.close()
        context.term()
        print("Server resources released.")


#RTSP 재연결 관련 함수
def reconnect_video_capture(in_url, retry_interval=5, max_retries=10):
    cap = cv2.VideoCapture(in_url) 
    retries = 0

    while not cap.isOpened() and retries < max_retries:
        print(f"Failed to connect to {in_url}. Retrying in {retry_interval} seconds...")
        time.sleep(retry_interval)
        cap = cv2.VideoCapture(in_url)
        retries += 1

    if cap.isOpened():
        # print("Successfully connected.")
        return cap
    else:
        print("Failed to reconnect after multiple attempts.")
        return None


# db 관련 함수
def get_connection():
    """
    Returns a new PostgreSQL connection.
    Manages connection errors and prints appropriate error messages.
    """
    try:
        # config 모듈 import (상대 경로로)
        import sys
        from pathlib import Path
        # ai_server 디렉토리를 경로에 추가
        ai_server_path = Path(__file__).parent.parent.parent
        if str(ai_server_path) not in sys.path:
            sys.path.insert(0, str(ai_server_path))
        from config.config import Config
        
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
        print("Connected to PostgreSQL successfully.")
        return conn
    except OperationalError as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None

#db roi값 반환
def get_roi(cctv_id):
    try:
        # PostgreSQL 데이터베이스에 연결
        connection = get_connection()
        cursor = connection.cursor()

        # SQL 쿼리 실행 (roi_id를 zone 번호로 사용)
        query = """
        select tcr.roi_id, tcr.point
        from public.tb_camera_roi tcr
        where tcr.camera_id = %s and tcr.model_nm = 'Detection'
        order by tcr.roi_id
        """
        cursor.execute(query, (cctv_id,))

        # 모든 ROI 행 가져오기 (카메라당 roi_id가 여러 개일 수 있음)
        results = cursor.fetchall()

        if not results:
            return None, None

        roi_list = []
        zone_list = []
        for row in results:
            roi_id = row[0]
            parsed = json.loads(row[1])
            if parsed:
                for poly in parsed:
                    roi_list.append(poly)
                    zone_list.append(int(roi_id))

        return (roi_list, zone_list) if roi_list else (None, None)

    except Exception as error:
        print(f"Error: {error}")
        return None, None

    finally:
        # 연결 종료
        if connection:
            cursor.close()
            connection.close()
        print("Closed to PostgreSQL successfully.")



#tb_camera_event_hist  CRUD 코드
#추가
def insert_camera_event_hist(event_time, camera_id, event_type, event_desc, file_path, isRead, remark):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO public.tb_camera_event_hist (event_time, camera_id, event_type, event_desc, file_path, isRead, remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (event_time, camera_id, event_type, event_desc, file_path, isRead, remark))

        conn.commit()
        print("Camera event history inserted successfully.")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")



def create_directory_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Directory '{path}' has been created.")
    else:
        print(f"Directory '{path}' already exists.")


def generate_coordinates_by_cams(all_raw_coordinates):
    """
    각 카메라별 Grid 좌표를 row-col 구조의 딕셔너리로 변환하여 반환합니다.
    """
    all_camera_grids = {}

    for camera_id, (raw_coordinates, sort_direction) in all_raw_coordinates.items():
        sorted_grid = {}
        rows = len(raw_coordinates)
        cols = len(raw_coordinates[0]) if rows > 0 else 0

        def extract_coordinates(point):
            """ Recursively extract (x, y) coordinate from deeply nested lists. """
            while isinstance(point, list) and len(point) == 1:
                point = point[0]
            if isinstance(point, list) and len(point) == 2 and all(isinstance(i, (int, float)) for i in point):
                return tuple(point)
            else:
                print(f"Invalid coordinate data for camera {camera_id}: {point}")
                return None

        for row_idx, row in enumerate(raw_coordinates):
            for col_idx, cell in enumerate(row):
                row_num = rows - row_idx - 1
                col_num = cols - col_idx - 1

                polygon = []
                for point in cell:
                    coord = extract_coordinates(point)
                    if coord:
                        polygon.append(coord)

                if row_num not in sorted_grid:
                    sorted_grid[row_num] = {}

                sorted_grid[row_num][col_num] = {
                    "coordinates": polygon,
                    "row": row_num,
                    "col": col_num
                }

        # ✅ 카메라별 변환된 데이터 저장
        all_camera_grids[camera_id] = {
            "sorted_grid": sorted_grid,
            "cols": cols,
            "rows": rows
        }

    return all_camera_grids  # 🔹 모든 카메라별 변환된 데이터를 반환


def get_raw_grid_coordinates_all_cam():
    """
    Fetches raw grid coordinates from the database for all cameras.
    Ensures the retrieved data is a valid list.
    """
    connection = None
    cursor = None
    all_camera_grids = {}  # 🔹 모든 카메라 데이터를 저장할 딕셔너리

    try:
        connection = get_connection()
        if connection is None:
            raise Exception("Database connection failed")

        cursor = connection.cursor()

        query = """
        SELECT camera_id, grid_data, sort_direction
        FROM public.tb_camera_grid
        """
        cursor.execute(query)
        results = cursor.fetchall()  # 🔹 모든 데이터를 가져옴

        if not results:
            return {}

        for row in results:
            camera_id = row[0]
            if not isinstance(row[1], list):
                raw_grid_coordinates = ast.literal_eval(row[1]) #배포 전용
            else:
                raw_grid_coordinates = row[1] #개발용
            sort_direction = row[2]

            # ✅ 데이터 유효성 검사
            if not isinstance(raw_grid_coordinates, list):
                print(f"❌ Invalid data format for camera {camera_id}: Expected list, got {type(raw_grid_coordinates)}")
                continue  # 유효하지 않은 데이터는 무시

            all_camera_grids[camera_id] = (raw_grid_coordinates, sort_direction)

        return all_camera_grids  # 🔹 모든 카메라의 그리드 데이터를 반환

    except Exception as error:
        print(f"❌ Database Error: {error}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Closed to PostgreSQL successfully.")


def get_raw_safety_grid_coordinates_all_cam():
    """
    Fetches raw grid coordinates from the database for all cameras.
    Ensures the retrieved data is a valid list.
    """
    connection = None
    cursor = None
    all_camera_grids = {}  # 🔹 모든 카메라 데이터를 저장할 딕셔너리

    try:
        connection = get_connection()
        if connection is None:
            raise Exception("Database connection failed")

        cursor = connection.cursor()

        query = """
        SELECT camera_id, grid_data, sort_direction
        FROM public.tb_camera_safety_grid
        """
        cursor.execute(query)
        results = cursor.fetchall()  # 🔹 모든 데이터를 가져옴

        if not results:
            return {}

        for row in results:
            camera_id = row[0]
            if not isinstance(row[1], list):
                raw_grid_coordinates = ast.literal_eval(row[1]) #배포 전용
            else:
                raw_grid_coordinates = row[1] #개발용
            sort_direction = row[2]

            # ✅ 데이터 유효성 검사
            if not isinstance(raw_grid_coordinates, list):
                print(f"❌ Invalid data format for camera {camera_id}: Expected list, got {type(raw_grid_coordinates)}")
                continue  # 유효하지 않은 데이터는 무시

            all_camera_grids[camera_id] = (raw_grid_coordinates, sort_direction)

        return all_camera_grids  # 🔹 모든 카메라의 그리드 데이터를 반환

    except Exception as error:
        print(f"❌ Database Error: {error}")
        return {}

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Closed to PostgreSQL successfully.")