import os
os.environ["MPLBACKEND"] = "Agg"
import psutil
import subprocess
import os
import atexit
import json
import requests
import bcrypt
from PIL import Image
import math
import psycopg2  # type: ignore
from psycopg2 import extras
from psycopg2.extras import execute_values
from datetime import timedelta, datetime
import platform
import sys
import base64
import io
import pandas as pd
from ortools.sat.python import cp_model
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.colors import to_rgba
from psycopg2.extras import Json
plt.ioff()  # 인터랙티브 OFF (선택)
# plt.show() 쓰지 말고:
plt.rc('font', family='NanumGothic')
plt.rcParams['axes.unicode_minus'] = False

import random
from flask import current_app, jsonify
from lib.db import get_connection
from collections import defaultdict, Counter


from rtsp_service.lib.public_func import generate_coordinates_by_cams, get_raw_grid_coordinates_all_cam, get_raw_safety_grid_coordinates_all_cam
import re

# --- utils ---
def _norm(v: object) -> str:
    """None/'NULL'/'null'/'None'/'-' 등을 공란으로 통일"""
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s in {"NULL", "null", "None", "-"} else s


# ----------------------------
# 3) 리스트 조회 (옵션 필터/페이징)
# ----------------------------
def fn_list_twin_detection_filters(camera_id: str | None = None, object_label: str | None = None,
                                   limit: int = 100, offset: int = 0):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        where = []
        params = []

        if camera_id:
            where.append("camera_id = %s")
            params.append(camera_id)
        if object_label:
            where.append("object_label = %s")
            params.append(object_label)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        sql = f"""
            SELECT tf.filter_id, tf.camera_id, tc."location" , tf.grid_width, tf.grid_height, tf.detected_row, tf.detected_col, tf.object_label, tf.created_at
            FROM public.tb_twin_detection_filter tf
            left join public.tb_camera tc on tc.camera_id = tf.camera_id 
            {where_sql}
            ORDER BY tf.created_at DESC, tf.filter_id DESC
            LIMIT %s OFFSET %s
        """
        params += [limit, offset]
        cur.execute(sql, params)
        rows = cur.fetchall()

        # total count (페이지네이션 필요 시)
        count_sql = f"SELECT COUNT(*) AS cnt FROM public.tb_twin_detection_filter {where_sql}"
        cur.execute(count_sql, params[:-2])
        total = cur.fetchone()["cnt"]

        return {"items": rows, "total": total, "limit": limit, "offset": offset}, None

    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()


# ----------------------------
# 오탐 객체 삭제
# ----------------------------

def fn_delete_twin_detection_filter(filter_id: int):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)
        sql = """
            DELETE FROM public.tb_twin_detection_filter
            WHERE filter_id = %s
            RETURNING filter_id, camera_id, grid_width, grid_height, detected_row, detected_col, object_label, created_at;
        """
        cur.execute(sql, (filter_id,))
        row = cur.fetchone()
        conn.commit()
        return row, None
    except Exception as e:
        if conn:
            conn.rollback()
        return None, str(e)
    finally:
        if conn:
            conn.close()



# ----------------------------
# 오탐 객체 추가
# ----------------------------
def fn_add_twin_detection_filter(camera_id: str, grid_width: int, grid_height: int,
                                 detected_row: int, detected_col: int, object_label: str):
    """
    고유 제약(uq_twin_detection_filter_unique_rule)에 의해 중복 방지.
    - 새 레코드 생성 시 {"created": True, "row": {...}}
    - 이미 존재하면 기존 레코드 반환 {"created": False, "row": {...}}
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        insert_sql = """
            INSERT INTO public.tb_twin_detection_filter
                (camera_id, grid_width, grid_height, detected_row, detected_col, object_label)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (camera_id, grid_width, grid_height, detected_row, detected_col, object_label)
            DO NOTHING
            RETURNING filter_id, camera_id, grid_width, grid_height, detected_row, detected_col, object_label, created_at;
        """
        cur.execute(insert_sql, (camera_id, grid_width, grid_height, detected_row, detected_col, object_label))
        row = cur.fetchone()

        if row is None:
            # 이미 존재 → 기존값 찾아서 반환
            sel_sql = """
                SELECT filter_id, camera_id, grid_width, grid_height, detected_row, detected_col, object_label, created_at
                FROM public.tb_twin_detection_filter
                WHERE camera_id=%s AND grid_width=%s AND grid_height=%s
                  AND detected_row=%s AND detected_col=%s AND object_label=%s
                LIMIT 1
            """
            cur.execute(sel_sql, (camera_id, grid_width, grid_height, detected_row, detected_col, object_label))
            row = cur.fetchone()
            conn.commit()
            return {"created": False, "row": row}, None
        else:
            conn.commit()
            return {"created": True, "row": row}, None

    except Exception as e:
        if conn:
            conn.rollback()
        return None, str(e)
    finally:
        if conn:
            conn.close()

def get_detection_label_changes(camera_id=None, start_date=None, end_date=None):
    """
    Returns a list of detection label changes from the tb_twin_detection_history table.
    
    This function executes a SQL query that:
    1. Identifies positions where object labels have changed
    2. Calculates the time between label changes
    3. Formats the execution time in hours, minutes, and seconds
    4. Calculates average execution times (total, by camera, by object_label)
    
    Args:
        camera_id (str, optional): Filter results by camera_id. If None, returns data for all cameras.
        start_date (str, optional): Filter results by start date (inclusive). Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.
        end_date (str, optional): Filter results by end date (inclusive). Format: 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.
    
    Returns:
        dict: A dictionary with status, data containing the label changes, and averages
    """
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Base query
        query = """
        WITH position_changes AS (
            SELECT
                camera_id,
                detected_row,
                detected_col,
                object_label,
                created_at,
                LAG(object_label) OVER (
                    PARTITION BY camera_id, detected_row, detected_col
                    ORDER BY created_at
                ) as prev_object_label,
                LAG(created_at) OVER (
                    PARTITION BY camera_id, detected_row, detected_col
                    ORDER BY created_at
                ) as prev_detection_time
            FROM tb_twin_detection_history
            {where_clause}
            ORDER BY camera_id, detected_row, detected_col, created_at
        ),
        label_changes AS (
            SELECT
                camera_id,
                detected_row,
                detected_col,
                prev_detection_time as start_time,
                created_at as end_time,
                prev_object_label as previous_label,
                object_label as current_label,
                EXTRACT(EPOCH FROM (created_at - prev_detection_time)) as execution_time_seconds
            FROM position_changes
            WHERE prev_object_label IS NOT NULL
              AND prev_object_label != object_label
        )
        SELECT
            camera_id,
            detected_row,
            detected_col,
            previous_label,
            current_label,
            start_time,
            end_time,
            execution_time_seconds
        FROM label_changes
        ORDER BY camera_id, detected_row, detected_col, start_time;
        """
        
        # Prepare filters
        filters = []
        
        # Add camera_id filter if provided
        if camera_id:
            filters.append(f"camera_id = '{camera_id}'")
            
        # Add date range filters if provided
        if start_date:
            filters.append(f"created_at >= '{start_date}'")
        if end_date:
            filters.append(f"created_at <= '{end_date}'")
            
        # Combine filters into WHERE clause
        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)
            
        # Format the query with the where clause
        query = query.format(where_clause=where_clause)
        
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            result_dict = {}
            for i, column in enumerate(columns):
                # Convert datetime objects to string for JSON serialization
                if isinstance(row[i], datetime):
                    result_dict[column] = row[i].isoformat()
                else:
                    result_dict[column] = row[i]
            
            # Add formatted_execution_time field using the format_execution_time helper function
            if 'execution_time_seconds' in result_dict and result_dict['execution_time_seconds'] is not None:
                result_dict['formatted_execution_time'] = format_execution_time(result_dict['execution_time_seconds'])
            else:
                result_dict['formatted_execution_time'] = "00:00:00"
                
            results.append(result_dict)
        
        # Calculate averages
        averages = {}
        
        # Total average execution time
        if results:
            total_seconds = sum(item['execution_time_seconds'] for item in results if item['execution_time_seconds'] is not None)
            count = len(results)
            
            avg_seconds = total_seconds / count if count > 0 else 0
            
            averages["total"] = {
                "avg_execution_time_seconds": avg_seconds,
                "avg_execution_time_minutes": avg_seconds / 60,
                "formatted_avg_execution_time": format_execution_time(avg_seconds),
                "count": count
            }
        else:
            averages["total"] = {
                "avg_execution_time_seconds": 0,
                "avg_execution_time_minutes": 0,
                "formatted_avg_execution_time": "0h 0m 0s",
                "count": 0
            }
        
        # Camera-specific average execution times
        camera_averages = {}
        for item in results:
            camera_id = item['camera_id']
            if camera_id not in camera_averages:
                camera_averages[camera_id] = {
                    "total_seconds": 0,
                    "count": 0
                }
            
            camera_averages[camera_id]["total_seconds"] += item['execution_time_seconds']
            camera_averages[camera_id]["count"] += 1
        
        # Calculate final camera averages
        averages["by_camera"] = {}
        for camera_id, data in camera_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            averages["by_camera"][camera_id] = {
                "avg_execution_time_seconds": avg_seconds,
                "avg_execution_time_minutes": avg_seconds / 60,
                "formatted_avg_execution_time": format_execution_time(avg_seconds),
                "count": data["count"]
            }
        
        # Object label-specific average execution times
        label_averages = {}
        for item in results:
            current_label = item['current_label']
            if current_label not in label_averages:
                label_averages[current_label] = {
                    "total_seconds": 0,
                    "count": 0
                }
            
            label_averages[current_label]["total_seconds"] += item['execution_time_seconds']
            label_averages[current_label]["count"] += 1
        
        # Calculate final object label averages
        averages["by_object_label"] = {}
        for current_label, data in label_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            averages["by_object_label"][current_label] = {
                "avg_execution_time_seconds": avg_seconds,
                "avg_execution_time_minutes": avg_seconds / 60,
                "formatted_avg_execution_time": format_execution_time(avg_seconds),
                "count": data["count"]
            }
        
        # Position-specific average execution times (by detected_row and detected_col)
        position_averages = {}
        for item in results:
            row = item['detected_row']
            col = item['detected_col']
            position_key = f"{row}_{col}"
            
            if position_key not in position_averages:
                position_averages[position_key] = {
                    "total_seconds": 0,
                    "count": 0,
                    "row": row,
                    "col": col
                }
            
            position_averages[position_key]["total_seconds"] += item['execution_time_seconds']
            position_averages[position_key]["count"] += 1
        
        # Calculate final position averages
        averages["by_position"] = {}
        for position_key, data in position_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            averages["by_position"][position_key] = {
                "detected_row": data["row"],
                "detected_col": data["col"],
                "avg_execution_time_seconds": avg_seconds,
                "avg_execution_time_minutes": avg_seconds / 60,
                "formatted_avg_execution_time": format_execution_time(avg_seconds),
                "count": data["count"]
            }
        
        return {
            "status": "success",
            "data": results,
            "averages": averages
        }
        
    except Exception as e:
        print(f"❌ Error retrieving detection label changes: {e}")
        return {
            "status": "error",
            "message": str(e)
        }
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def format_execution_time(seconds):
    """
    Formats execution time in seconds to a string in the format "HH:MM:SS"
    
    Args:
        seconds (float): Time in seconds
        
    Returns:
        str: Formatted time string in HH:MM:SS format
    """
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    remaining_seconds = int(seconds % 60)
    
    return f"{hours:02d}:{minutes:02d}:{remaining_seconds:02d}"

# 현재 파일이 있는 디렉토리 경로
current_dir = os.path.dirname(os.path.abspath(__file__))

# 상위 디렉토리 경로
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))

# 상위 디렉토리를 sys.path에 추가
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 이제 상위 폴더에 있는 라이브러리를 임포트할 수 있습니다.

# python_path = "/home/jinmj/miniconda3/envs/panenv/bin/python3.10"
python_path = "python3"
# python_path = '/home/panadmin/miniconda3/envs/panenv/bin/python3.10'

if os.name == 'nt':  # Windows
    python_path = "C:/projects/trmps_sever/envs/trmps/python.exe"

# 프로세스 list
process_dict = {}  # PID를 저장하는 딕셔너리

# 이미지 전송용 소켓 통신 포트 list 
port_dict = {}  # port를 저장하는 딕셔너리

# ---------------------------------------------
# NULL 아닌 셀 상세 전체 조회 함수
# ---------------------------------------------
def fn_list_filled_work_space_cell_detail():
    """
    work_space_nm 또는 product_nm이 NULL이 아닌 데이터 전체 반환
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT cell_cd, work_space_cd, work_space_nm, product_nm, remark, reg_dt
            FROM public.tb_work_space_cell_detail
            WHERE work_space_nm IS NOT NULL OR product_nm IS NOT NULL
            ORDER BY work_space_cd, cell_cd
        """
        cur.execute(query)
        rows = cur.fetchall()
        return rows, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()
            
# ---------------------------------------------
# 부분 업데이트 함수 (PATCH)
# ---------------------------------------------
def fn_update_work_space_cell_detail(cell_cd: str, work_space_nm=None, product_nm=None, remark=None):
    """
    전달된 필드만 부분 업데이트.
    업데이트 가능 컬럼: work_space_nm, product_nm, remark
    반환: 업데이트 후 행(dict)
    """
    conn = None
    try:
        updates = []
        params = []

        if work_space_nm is not None:
            updates.append("work_space_nm = %s")
            params.append(work_space_nm)

        if product_nm is not None:
            updates.append("product_nm = %s")
            params.append(product_nm)

        if remark is not None:
            updates.append("remark = %s")
            params.append(remark)

        if not updates:
            return None, "업데이트할 필드가 없습니다."

        params.append(cell_cd)

        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        sql = f"""
            UPDATE public.tb_work_space_cell_detail
            SET {", ".join(updates)}
            WHERE cell_cd = %s
            RETURNING cell_cd, work_space_cd, work_space_nm, product_nm, remark, reg_dt;
        """
        cur.execute(sql, params)
        row = cur.fetchone()
        conn.commit()

        if not row:
            return None, "해당 cell_cd 데이터가 없습니다."

        return row, None

    except Exception as e:
        if conn:
            conn.rollback()
        return None, str(e)
    finally:
        if conn:
            conn.close()

# ---------------------------------------------
# 단건 조회 함수
# ---------------------------------------------
def fn_get_work_space_cell_detail(cell_cd: str):
    """
    tb_work_space_cell_detail 테이블에서 특정 cell_cd 행 조회
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT cell_cd, work_space_cd, work_space_nm, product_nm, remark, reg_dt
            FROM public.tb_work_space_cell_detail
            WHERE cell_cd = %s
        """
        cur.execute(query, (cell_cd,))
        row = cur.fetchone()
        return row, None

    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()

# ---------------------------------------------
# 리스트 조회 함수
# ---------------------------------------------
def fn_list_work_space_cell_detail(work_space_cd: str):
    """
    tb_work_space_cell_detail에서 work_space_cd로 필터하여
    cell_cd, work_space_nm, product_nm 컬럼만 리스트로 반환
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT cell_cd, work_space_nm, product_nm
            FROM public.tb_work_space_cell_detail
            WHERE work_space_cd = %s
            ORDER BY cell_cd
        """
        cur.execute(query, (work_space_cd,))
        rows = cur.fetchall()  # 리스트
        return rows, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()


# ---------------------------------------------
# 단건 조회 함수
# ---------------------------------------------
def fn_get_work_space_cell(work_space_cd: str):
    """
    work_space_cd에 해당하는 tb_work_space_cell 1개 행 조회
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT work_space_cd, cell_count_x, cell_count_y, total_cells, reg_dt
            FROM public.tb_work_space_cell
            WHERE work_space_cd = %s
        """
        cur.execute(query, (work_space_cd,))
        row = cur.fetchone()  # 1개 행만 가져옴
        return row, None
    except Exception as e:
        return None, str(e)
    finally:
        if conn:
            conn.close()


def fn_upsert_work_space_cell(work_space_cd: str, cell_x: int, cell_y: int):
    """
    1) tb_work_space_cell UPSERT
    2) X/Y가 신규 또는 변경된 경우:
       - tb_work_space_cell_detail에서 해당 work_space_cd 전부 삭제
       - {WS_CD}{열}{행} 규칙으로 cell_cd를 생성해 벌크 INSERT (다른 컬럼은 NULL)
    반환: upsert 결과 행(dict)
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        # 기존 값 조회
        cur.execute(
            """
            SELECT cell_count_x, cell_count_y
            FROM public.tb_work_space_cell
            WHERE work_space_cd = %s
            """,
            (work_space_cd,)
        )
        old = cur.fetchone()

        # UPSERT
        upsert_sql = """
            INSERT INTO public.tb_work_space_cell
                (work_space_cd, cell_count_x, cell_count_y)
            VALUES (%s, %s, %s)
            ON CONFLICT (work_space_cd)
            DO UPDATE SET
                cell_count_x = EXCLUDED.cell_count_x,
                cell_count_y = EXCLUDED.cell_count_y,
                reg_dt       = NOW()
            RETURNING work_space_cd, cell_count_x, cell_count_y, total_cells, reg_dt;
        """
        cur.execute(upsert_sql, (work_space_cd, cell_x, cell_y))
        row = cur.fetchone()

        # 변경 감지: 신규(없었음) 또는 X/Y 값이 달라졌으면 디테일 재생성
        need_regen = (old is None) or (old["cell_count_x"] != cell_x) or (old["cell_count_y"] != cell_y)
        if need_regen:
            # 1) 기존 detail 삭제
            cur.execute(
                "DELETE FROM public.tb_work_space_cell_detail WHERE work_space_cd = %s",
                (work_space_cd,)
            )

            # 2) 새 cell_cd 생성 (열 우선: x 고정, y 증가 → 예: AF11, AF12, AF21, AF22)
            values = []
            for x in range(1, cell_x + 1):
                for y in range(1, cell_y + 1):
                    cell_cd = f"{work_space_cd}{x}{y}"
                    # (work_space_nm, product_nm, remark) = NULL 삽입
                    values.append((cell_cd, work_space_cd, None, None, None))

            if values:
                insert_sql = """
                    INSERT INTO public.tb_work_space_cell_detail
                        (cell_cd, work_space_cd, work_space_nm, product_nm, remark)
                    VALUES %s
                """
                execute_values(cur, insert_sql, values, page_size=1000)

        conn.commit()
        return row, None

    except Exception as e:
        if conn:
            conn.rollback()
        return None, str(e)
    finally:
        if conn:
            conn.close()


# ---------------------------------------------
# 전체 작업공간 조회 함수
# ---------------------------------------------
def fn_get_work_space_cell_detail(product_nm:str) -> list:
    """
    tb_work_space 테이블 전체 데이터를 반환
    """
    conn = None
    try:
        conn = get_connection()

        query = """
            SELECT work_space_nm
            FROM public.tb_work_space_cell_detail
            where product_nm = %s
            ORDER BY cell_cd
        """
        df = pd.read_sql(query, conn, params=(product_nm,))

        return df.to_dict(orient='records')

    except Exception as e:
        print("❌ 작업공간 조회 오류:", e)
        return []

    finally:
        if conn:
            conn.close()
        print("✅ PostgreSQL 연결 종료")


# ---------------------------------------------
# 전체 작업공간 조회 함수
# ---------------------------------------------
def fn_get_work_space() -> list:
    """
    tb_work_space 테이블 전체 데이터를 반환
    """
    conn = None
    try:
        conn = get_connection()

        query = """
            SELECT work_space_cd, work_space_nm, remark
            FROM public.tb_work_space
            ORDER BY work_space_cd
        """
        df = pd.read_sql(query, conn)

        return df.to_dict(orient='records')

    except Exception as e:
        print("❌ 작업공간 조회 오류:", e)
        return []

    finally:
        if conn:
            conn.close()
        print("✅ PostgreSQL 연결 종료")



def get_sim_list_df(sim_id: int,  start_date = None, end_date = None) -> pd.DataFrame:
    """
    sim_id에 해당하는 dt_sim_list 데이터를 DataFrame으로 반환
    """
    try:
        conn = get_connection()
        if start_date is not None and end_date is not None:
            query = """
                SELECT *
                FROM public.dt_sim_list
                WHERE sim_id = %s and
                      start_date >= %s and
                      due_date <= %s 
                ORDER BY job_id, mac_id
            """
            df = pd.read_sql(query, conn, params=(sim_id, start_date, end_date))
            return df
        else:
            query = """
                SELECT *
                FROM public.dt_sim_list
                WHERE sim_id = %s
                ORDER BY job_id, mac_id
            """
            df = pd.read_sql(query, conn, params=(sim_id,))
            return df

    except Exception as e:
        print("❌ sim_list 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def get_sim_job_df(sim_id: int) -> pd.DataFrame:
    """
    sim_id에 해당하는 dt_sim_job 데이터를 DataFrame으로 반환
    """
    try:
        conn = get_connection()
        query = """
            SELECT sim_id, job_id, job_name
            FROM public.dt_sim_job
            WHERE sim_id = %s
            ORDER BY job_id
        """
        df = pd.read_sql(query, conn, params=(sim_id,))
        return df

    except Exception as e:
        print("❌ sim_job 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_legacy_bwts_runing_work() -> pd.DataFrame:
    """
    현재 작업 진행중은 bwts 데이터 목록 반환환
    """
    # 매핑 딕셔너리
    status_mapping = {
        'O': '진행중',
        'S': '시작',
        'N': '중단',
        'C': '종료',
        'W': '대기',
        '_0': '無',
        '_1': '작지X'
    }

    try:
        conn = get_connection()
        # query = """
        #     SELECT *
        #     FROM public.dt_leg_ord_data_bwts bwts
        #     where not ((bwts.filter_sts = 'C' or bwts.filter_sts = '_0'  ) and (bwts.skid_sts = 'C' or bwts.skid_sts = '_0'  ) and (bwts.uv_sts  = 'C' or bwts.uv_sts  = '_0'))
        #     order by ordnum desc, ordseq DESC 
        # """
        query = """
            SELECT *
            FROM public.dt_leg_ord_data_bwts bwts
            where not (bwts.skid_sts = 'C' or bwts.skid_sts = '_0' or bwts.skid_sts = '_1')
            order by ordnum desc, ordseq DESC 
        """
        
        df = pd.read_sql(query, conn, params=())

        df['ordseq'] = df['ordseq'].dropna().astype(int)
        df['filter_sts'] = df['filter_sts'].replace(status_mapping)
        df['skid_sts'] = df['skid_sts'].replace(status_mapping)
        df['uv_sts'] = df['uv_sts'].replace(status_mapping)

        return df

    except Exception as e:
        print("❌ sim_job 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_hullno_by_ordnum_ordseq(ordnum, ordseq):
    """
    수주번호(ordnum)와 순번(ordseq)을 기준으로 호선번호(hullno)를 반환

    Args:
        ordnum (str): 수주번호
        ordseq (str or int): 순번

    Returns:
        str: 호선번호(hullno). 데이터가 없는 경우 None 반환
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            query = """
                SELECT hullno
                FROM public.dt_leg_ord_data_bwts
                WHERE ordnum=%s AND ordseq=%s
                LIMIT 1;
            """
            cur.execute(query, (ordnum, ordseq))
            result = cur.fetchone()

            if result:
                return result[0]
            else:
                return None

    except Exception as e:
        print("❌ hullno 조회 오류:", e)
        return None

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_location_by_detail_location_ordseq(detail_location, ordseq):
    """
    상세위치(detail_location)와 순번(ordseq)을 기준으로 위치(location)를 반환

    Args:
        detail_location (str): 상세위치 (예: 'camera_id_R1C2_3x4')
        ordseq (str or int): 순번

    Returns:
        str: 위치(location). 데이터가 없는 경우 None 반환
    """
    conn = None
    try:
        # 상세위치에서 camera_id 추출
        if detail_location and '_' in detail_location:
            camera_id = detail_location.split('_')[0]

            conn = get_connection()
            with conn.cursor() as cur:
                query = """
                    SELECT location
                    FROM public.tb_camera
                    WHERE camera_id=%s
                    LIMIT 1;
                """
                cur.execute(query, (camera_id,))
                result = cur.fetchone()

                if result:
                    return result[0]
        return None

    except Exception as e:
        print("❌ location 조회 오류:", e)
        return None

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


#검출 데이터 수주정보 연결
def fn_set_pro_oder_link(image_id, detection_id, ordnum=None, ordseq=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 새 order_no 조합
        new_order_no = None
        if ordnum and ordseq:
            new_order_no = f"{ordnum}-{ordseq}"

        if new_order_no:
            # 1) 선처리: 동일 order_no가 이미 존재하면 먼저 NULL 로 초기화
            cursor.execute("""
                UPDATE public.tb_twin_detection
                SET order_no = NULL
                WHERE order_no = %s;
            """, (new_order_no,))

            # 2) 대상 행에 새 order_no 설정
            cursor.execute("""
                UPDATE public.tb_twin_detection
                SET order_no = %s
                WHERE image_id = %s AND detection_id = %s;
            """, (new_order_no, image_id, detection_id))
        else:
            # ordnum/ordseq 없으면: 대상 행만 NULL 로 초기화
            cursor.execute("""
                UPDATE public.tb_twin_detection
                SET order_no = NULL
                WHERE image_id = %s AND detection_id = %s;
            """, (image_id, detection_id))

        affected_rows = cursor.rowcount
        conn.commit()

        if affected_rows > 0:
            print(f"Update successful. {affected_rows} row(s) affected.")
            return True
        else:
            print("No rows were updated. Check keys or values.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")


#사용자 입력 데이터 수주정보 연결
# 사용자 입력 데이터 수주정보 연결/해제 (중복 선제거 + NULL 처리 포함)
def fn_set_input_oder_link(image_id, detection_id, ordnum=None, ordseq=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 새 order_no 조합 (둘 다 있어야 설정, 하나라도 없으면 NULL 처리)
        new_order_no = f"{ordnum}-{ordseq}" if ordnum and ordseq else None

        if new_order_no:
            # 1) 선처리: 동일 order_no가 이미 다른 행에 있으면 NULL 로 초기화
            cursor.execute("""
                UPDATE public.tb_twin_user_input
                SET order_no = NULL
                WHERE order_no = %s;
            """, (new_order_no,))

            # 2) 대상 행에 새 order_no 설정
            cursor.execute("""
                UPDATE public.tb_twin_user_input
                SET order_no = %s
                WHERE image_id = %s AND input_id = %s;
            """, (new_order_no, image_id, detection_id))
        else:
            # ordnum/ordseq가 비어 있으면: 대상 행의 order_no 만 NULL 로 초기화
            cursor.execute("""
                UPDATE public.tb_twin_user_input
                SET order_no = NULL
                WHERE image_id = %s AND input_id = %s;
            """, (image_id, detection_id))

        affected_rows = cursor.rowcount
        conn.commit()

        if affected_rows > 0:
            print(f"Update successful. {affected_rows} row(s) affected.")
            return True
        else:
            print("No rows were updated. Check if image_id/input_id exists.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")


def fn_get_legacy_scrubber_runing_work() -> pd.DataFrame:
    """
    현재 작업 진행중은 scrubber 데이터 목록 반환환
    """
    # 매핑 딕셔너리
    status_mapping = {
        'O': '진행중',
        'S': '시작',
        'H': '중단',
        'C': '종료',
        'W': '대기',
        '_0': '無',
        '_1': '작지X'
    }

    try:
        conn = get_connection()
        # query = """
        #     SELECT *
        #     FROM public.dt_leg_ord_data_scrubber scrubber
        #     where not ((scrubber.cutting_sts = 'C' or scrubber.cutting_sts = '_0') 
        #                 and (scrubber.bending_sts = 'C' or scrubber.bending_sts = '_0') 
        #                 and (scrubber.fit_wel_sts  = 'C' or scrubber.fit_wel_sts  = '_0') 
        #                 and (scrubber.pt_sts  = 'C' or scrubber.pt_sts  = '_0') 
        #                 and (scrubber.vl_dl_sts  = 'C' or scrubber.vl_dl_sts  = '_0') 
        #                 and (scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
        #                 and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
        #                 and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
        #                 and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0')
        #                 )
        #     order by ordnum desc, ordseq DESC 
        # """

        query = """
            SELECT *
            FROM public.dt_leg_ord_data_scrubber scrubber
            where not ((scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
                        and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
                        and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
                        and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0')
                        ) and (scrubber.cutting_sts = 'C' and 
                               scrubber.bending_sts = 'C' and
                               scrubber.fit_wel_sts  = 'C' and
                               scrubber.pt_sts  = 'C' and
                               scrubber.vl_dl_sts  = 'C')
            order by ordnum desc, ordseq DESC 
        """
        df = pd.read_sql(query, conn, params=())

        df['ordseq'] = df['ordseq'].dropna().astype(int)
        df['cutting_sts'] = df['cutting_sts'].replace(status_mapping)
        df['bending_sts'] = df['bending_sts'].replace(status_mapping)
        df['fit_wel_sts'] = df['fit_wel_sts'].replace(status_mapping)
        df['pt_sts'] = df['pt_sts'].replace(status_mapping)
        df['vl_dl_sts'] = df['vl_dl_sts'].replace(status_mapping)
        df['acid_sts'] = df['acid_sts'].replace(status_mapping)
        df['ass_sts'] = df['ass_sts'].replace(status_mapping)
        df['insp_sts'] = df['insp_sts'].replace(status_mapping)
        df['pack_sts'] = df['pack_sts'].replace(status_mapping)

        return df

    except Exception as e:
        print("❌ sim_job 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_legacy_scrubber_at_ordnum(ordnum_ordseq) -> pd.DataFrame:
    """
    현재 작업 진행중은 scrubber 데이터 목록 반환환
    """
    # 매핑 딕셔너리
    status_mapping = {
        'E': '확정',
        'F': '마감',
        'O': '지시',
        'S': '시작',
        'H': '중단',
        'C': '종료'
    }

    ordnum = ordnum_ordseq.split("-")[0]
    ordseq = ordnum_ordseq.split("-")[1]

    try:
        conn = get_connection()
        query = """
            SELECT *
            FROM public.dt_leg_ord_data_scrubber scrubber
            where ordnum=%s AND ordseq=%s
            order by ordnum desc, ordseq DESC;
        """
        df = pd.read_sql(query, conn, params=(ordnum,ordseq))

        df['ordseq'] = df['ordseq'].dropna().astype(int)
        df['cutting_sts'] = df['cutting_sts'].replace(status_mapping)
        df['bending_sts'] = df['bending_sts'].replace(status_mapping)
        df['fit_wel_sts'] = df['fit_wel_sts'].replace(status_mapping)
        df['pt_sts'] = df['pt_sts'].replace(status_mapping)
        df['vl_dl_sts'] = df['vl_dl_sts'].replace(status_mapping)
        df['acid_sts'] = df['acid_sts'].replace(status_mapping)
        df['ass_sts'] = df['ass_sts'].replace(status_mapping)
        df['insp_sts'] = df['insp_sts'].replace(status_mapping)
        df['pack_sts'] = df['pack_sts'].replace(status_mapping)

        return df

    except Exception as e:
        print("❌ sim_job 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def get_sim_mac_df(sim_id: int) -> pd.DataFrame:
    """
    sim_id에 해당하는 dt_sim_mac 데이터를 DataFrame으로 반환
    """
    try:
        conn = get_connection()
        query = """
            SELECT sim_id, mac_id, mac_name
            FROM public.dt_sim_mac
            WHERE sim_id = %s
            ORDER BY mac_id
        """
        df = pd.read_sql(query, conn, params=(sim_id,))
        return df

    except Exception as e:
        print("❌ sim_mac 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def get_sim_input_df(sim_id: int) -> pd.DataFrame:
    """
    sim_id에 해당하는 dt_sim_input 데이터를 DataFrame으로 반환
    """
    try:
        conn = get_connection()
        query = """
            SELECT *
            FROM public.dt_sim_input
            WHERE sim_id = %s
        """
        df = pd.read_sql(query, conn, params=(sim_id,))
        return df

    except Exception as e:
        print("❌ sim_mac 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def get_sim_master_created_at(sim_id: int) -> str:
    """
    sim_id에 해당하는 dt_sim_master 데이터를 DataFrame으로 반환
    """
    try:
        conn = get_connection()
        query = """
            SELECT created_at
            FROM public.dt_sim_master
            WHERE sim_id = %s
        """
        df = pd.read_sql(query, conn, params=(sim_id,))

        return df.loc[0, "created_at"]

    except Exception as e:
        print("❌ sim_mac 조회 오류:", e)
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_sim_result(sim_id, base_time = None, start_date = None, end_date = None ):
    #시뮬레이션 결과
    if start_date != None and end_date != None:
        sim_result_df = get_sim_list_df(sim_id, start_date, end_date)
    else:
        sim_result_df = get_sim_list_df(sim_id)
    #job name list
    job_df = get_sim_job_df(sim_id)
    #mac_df
    mac_df = get_sim_mac_df(sim_id)

    #사용자 입력 데이터 받기
    input_df = get_sim_input_df(sim_id)

    job_names = job_df['job_name'].values
    # print('job_names')
    # print(job_names)
    machine_names = mac_df['mac_name'].values
    num_machines = len(machine_names)
    # 시각화용 y축: Machine 인덱스를 숫자로 정리
    machine_id_to_name = {i: name for i, name in enumerate(machine_names)}
    # print('mac_name')
    # print(mac_name)

    # 작업준비시간 (Setup Time) sim_run_df함수와 동일하게 설정해야됩
    setup_time = 1
    
    if base_time == None:
        # sim_id에 해당하는 기준일 반환
        start_date = get_sim_master_created_at(sim_id).strftime('%Y-%m-%d')
    
    # 작업시간 계산
    ## 초기작업의 작업진행률
    progress_rates = input_df['rate'].to_list()
    ## 초기작업의 인덱스
    progressing_list = list(filter(lambda x: progress_rates[x] != 0, range(len(progress_rates))))

    # 그래프폭 설정
    # 문자열을 datetime으로 변환
    sim_result_df["due_date"] = pd.to_datetime(sim_result_df["due_date"])

    # 가장 미래 날짜 추출
    end_day_temp = sim_result_df["due_date"].max()
    
    # print('sim_result_df')
    # print(sim_result_df)
    display_day = (end_day_temp- datetime.strptime(start_date, '%Y-%m-%d')).days
    # print('display_day')
    # print(display_day)

    max_width = 150
    max_day = 1300
    min_width = 9.45
    width = max_width * (display_day / max_day)

    #최소 폭
    if width < min_width:
        width = min_width

    fig, ax = plt.subplots(figsize=(round(width), 9))
    for idx, row in sim_result_df.iterrows():
        m_id = row["machine_id"]
        order = row["order"]
        max_order = sim_result_df[sim_result_df["machine_id"] == m_id]["order"].max()
        alpha = 1.0 - (0.7 * (order / max(1, max_order)))
        bar_height = 0.3
        job_name = job_names[idx]
        # job_num = re.findall(r'\d+', job_name)[0]

        # setup, processing 바 구분
        setup_duration = timedelta(setup_time)
        setup_color = to_rgba('orange', alpha)
        processing_duration = row["end_date"] - row["start_date"] - setup_duration
        processing_color = to_rgba('black', alpha)

        # 작업 바 그리기
        ## 초기 작업
        if idx in progressing_list:
            ## processing time 부분
            ax.barh(m_id, processing_duration + setup_duration,
                    left=row["start_date"], color='red',
                    edgecolor='red', height=bar_height)
        ## 스케줄대상 작업
        else:
            ## setup time 부분  (초기작업들은 작업시간만큼 다 blue로 그려야 한다.)
            ax.barh(m_id, setup_duration,
                    left=row["start_date"], color=setup_color,
                    edgecolor='black', height=bar_height)
            ## processing time 부분
            ax.barh(m_id, processing_duration,
                    left=row["start_date"]+setup_duration, color=processing_color,
                    edgecolor='black', height=bar_height)

        # 작업명 텍스트
        mid = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
        ax.text(mid, m_id + bar_height, f'{job_name}',
                ha='center', fontsize=6)

        # Ready Time 표시
        ax.plot(row["ready_date"], m_id + bar_height, marker='v', color='green', markersize=6)
        ax.text(row["ready_date"], m_id + bar_height + 0.1,
                f'{row["ready_date"].strftime("%m/%d")}({job_name})',
                color='green', ha='center', fontsize=6)

        # Due Date 표시
        ax.plot(row["due_date"], m_id - bar_height, marker='^', color='red', markersize=6)
        ax.text(row["due_date"], m_id - bar_height - 0.2,
                f'{row["due_date"].strftime("%m/%d")}({job_name})',
                color='red', ha='center', fontsize=6)

        # ★ 별표 조건: 납기 전에 끝났지만 다음 작업이 너무 일찍 시작된 경우
        if row["end"] < row["due"]:
            # 같은 기계에서 작업들 가져오기
            same_machine_df = sim_result_df[sim_result_df["mac_id"] == m_id].sort_values("start")
            this_order = row["order"]    # Cell내에서 현재 작업의 순번
            # 다음 작업이 있는 경우만 검사
            if this_order + 1 < len(same_machine_df):
                next_task = same_machine_df.iloc[this_order + 1]
                # 다음 작업 시작이 납기 이전이면 별 표시
                if next_task["start"] == row["end"]:
                    ax.text(mid, m_id, '★',
                            ha='center', va='bottom', color='blue', fontsize=12)

    # y축 설정: 숫자로 처리하고 라벨은 작업장 이름
    ax.set_yticks(range(num_machines))
    ax.set_yticklabels([machine_id_to_name[i] for i in range(num_machines)])

    # 날짜 형식 설정
    ax.xaxis.set_major_locator(mdates.WeekdayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    # 기준일 강조선
    highlight_date = datetime.strptime(base_time, "%Y-%m-%d")
    ax.axvline(highlight_date, color='orange', linestyle='--', linewidth=2,
            label='base date: ' + base_time)

    # 마무리
    ax.set_xlabel("Date")
    ax.set_ylabel("Machine")
    ax.set_title("Result")
    ax.legend()
    plt.xticks(rotation=45)
    ax.grid(True, linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # 버퍼에 이미지 저장 (PNG 형식)
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)

    # base64로 인코딩
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()
    plt.close()

    return img_base64

# def fn_get_sim_result(sim_id, base_date):
#     #시뮬레이션 결과
#     sim_result_df = get_sim_list_df(sim_id)
#     #job name list
#     job_df = get_sim_job_df(sim_id)
#     #mac_df
#     mac_df = get_sim_mac_df(sim_id)

#     job_names = job_df['job_name'].values
#     # print('job_names')
#     # print(job_names)
#     machine_names = mac_df['mac_name'].values
#     # print('mac_name')
#     # print(mac_name)

#     fig, ax = plt.subplots(figsize=(16, 10))
#     for _, row in sim_result_df.iterrows():
#         machine = row["mac_id"]
#         order = row["order"]
#         max_order = sim_result_df[sim_result_df["mac_id"] == machine]["order"].max()
#         alpha = 1.0 - (0.7 * (order / max(1, max_order)))
#         color = to_rgba('blue', alpha)
#         ax.barh(machine, row["end_date"] - row["start_date"], left=row["start_date"], color=color, edgecolor='black', height=0.3)
#         mid = row["start_date"] + (row["end_date"] - row["start_date"]) / 2
#         ax.text(mid, machine + 0.2, f'{job_names[int(row["job_id"])]}', ha='center', fontsize=8)
#         ax.plot(row["ready_date"], machine + 0.15, marker='v', color='green', markersize=6)
#         ax.text(row["ready_date"], machine + 0.3, f'R_{row["job_id"]+1}={row["ready_date"].strftime("%m-%d")}', color='green', ha='center', fontsize=6)
#         ax.plot(row["due_date"], machine - 0.15, marker='^', color='red', markersize=6)
#         ax.text(row["due_date"], machine - 0.3, f'D_{row["job_id"]+1}={row["due_date"].strftime("%m-%d")}', color='red', ha='center', fontsize=6)

#     ax.set_xlabel("Date")
#     ax.set_ylabel("Machine")
#     ax.set_title("Result")
#     ax.set_yticks(range(len(machine_names)))
#     ax.set_yticklabels([machine_names[m] for m in range(len(machine_names))])
#     ax.xaxis.set_major_locator(mdates.WeekdayLocator())
#     ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
#     # ✅ 특정 기준일 표시: 예) 2020년 6월 1일
#     highlight_date = datetime.strptime(base_date, "%Y-%m-%d")
#     ax.axvline(highlight_date, color='orange', linestyle='--', linewidth=2, label='base date: ' + base_date)
#     ax.legend()
#     plt.xticks(rotation=45)
#     ax.grid(True, linestyle='--', linewidth=0.5)
#     plt.tight_layout()

#     # 버퍼에 이미지 저장 (PNG 형식)
#     buf = io.BytesIO()
#     plt.savefig(buf, format='png')
#     buf.seek(0)

#     # base64로 인코딩
#     img_base64 = base64.b64encode(buf.read()).decode('utf-8')
#     buf.close()
#     plt.close()

#     return img_base64

def get_sim_master_between(start_day: str, end_day: str, type_code: str):
    """
    dt_sim_master 테이블에서 created_at(생성일)가 start_day ~ (end_day + 1) 사이인 항목을 조회합니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if type_code == 'BWTS':
                query = """
                    SELECT
                        m.sim_id,
                        m.free_mac,
                        m.relaxed_time,
                        m.created_at,
                        m.created_by,
                        m.sim_process,
                        CASE m.sim_process
                            WHEN 'OPTIMAL' THEN 'OPTIMAL: 모든 제약을 만족하는 전역 최적해'
                            WHEN 'FEASIBLE' THEN 'FEASIBLE: 제약을 만족하는 실행 가능 해(최적 보장은 아님)'
                            WHEN 'INFEASIBLE' THEN 'INFEASIBLE: 제약을 모두 만족하는 스케줄이 존재하지 않음'
                            WHEN 'MODEL_INVALID' THEN 'MODEL_INVALID: 모델 정의/제약 설정 오류'
                            WHEN 'UNKNOWN' THEN 'UNKNOWN: 탐색 중단 등으로 결과를 신뢰할 수 없음'
                            ELSE '상태 미지정'
                        END AS sim_process_desc,
                        COALESCE(i.총수주, 0) AS 총수주,
                        COALESCE(i.진행, 0) AS 진행,
                        MIN(dsl.start_date) as 시작일,
                        MAX(dsl.due_date) as 최종납기일
                    FROM 
                        public.dt_sim_master m
                    LEFT JOIN (
                        SELECT 
                            sim_id,
                            COUNT(*) AS 총수주,
                            COUNT(detail_location) AS 진행
                        FROM 
                            public.dt_sim_input
                        GROUP BY 
                            sim_id
                    ) i ON m.sim_id = i.sim_id
                    left join public.dt_sim_list dsl on m.sim_id = dsl.sim_id
                    WHERE 
                        m.created_at BETWEEN %s AND %s and (type_code = %s or type_code is null)
                    group by 
                        m.sim_id,
                        m.free_mac,
                        m.relaxed_time,
                        m.created_at,
                        m.created_by,
                        m.sim_process,
                        COALESCE(i.총수주, 0),
                        COALESCE(i.진행, 0)
                    ORDER BY 
                        m.sim_id;
                """
            else:
                query = """
                    SELECT
                        m.sim_id,
                        m.free_mac,
                        m.relaxed_time,
                        m.created_at,
                        m.created_by,
                        m.sim_process,
                        CASE m.sim_process
                            WHEN 'OPTIMAL' THEN 'OPTIMAL: 모든 제약을 만족하는 전역 최적해'
                            WHEN 'FEASIBLE' THEN 'FEASIBLE: 제약을 만족하는 실행 가능 해(최적 보장은 아님)'
                            WHEN 'INFEASIBLE' THEN 'INFEASIBLE: 제약을 모두 만족하는 스케줄이 존재하지 않음'
                            WHEN 'MODEL_INVALID' THEN 'MODEL_INVALID: 모델 정의/제약 설정 오류'
                            WHEN 'UNKNOWN' THEN 'UNKNOWN: 탐색 중단 등으로 결과를 신뢰할 수 없음'
                            ELSE '상태 미지정'
                        END AS sim_process_desc,
                        COALESCE(i.총수주, 0) AS 총수주,
                        COALESCE(i.진행, 0) AS 진행,
                        MIN(dsl.start_date) as 시작일,
                        MAX(dsl.due_date) as 최종납기일
                    FROM 
                        public.dt_sim_master m
                    LEFT JOIN (
                        SELECT 
                            sim_id,
                            COUNT(*) AS 총수주,
                            COUNT(detail_location) AS 진행
                        FROM 
                            public.dt_sim_input
                        GROUP BY 
                            sim_id
                    ) i ON m.sim_id = i.sim_id
                    left join public.dt_sim_list dsl on m.sim_id = dsl.sim_id
                    WHERE 
                        m.created_at BETWEEN %s AND %s and type_code = %s
                    group by 
                        m.sim_id,
                        m.free_mac,
                        m.relaxed_time,
                        m.created_at,
                        m.created_by,
                        m.sim_process,
                        COALESCE(i.총수주, 0),
                        COALESCE(i.진행, 0)
                    ORDER BY 
                        m.sim_id;
                """
            # end_day + 1일
            enddt = datetime.strptime(end_day, "%Y-%m-%d") + timedelta(days=1)
            cur.execute(query, (start_day, enddt.strftime("%Y-%m-%d"), type_code))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            return result

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return []

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_order_pro(location=None, sim_id=None, base_time=None):
    """
    location기준으로 수주진행 현황 데이터를 로드
    sim_id와 base_time가 모두 제공되면 dt_sim_list에서 sim_id에 해당하는 값을 모두 반환
    location도 sim_id와 base_time와 함께 입력된 경우 dt_sim_mac에서 location과 동일한 mac_name을 찾고
    sim_id와 mac_name에 해당하는 mac_id를 찾아 dt_sim_list에서 해당 mac_id와 기타 input에 해당하는 결과를 반환
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # location, sim_id, base_time가 모두 제공된 경우
            if location is not None and sim_id is not None and base_time is not None:
                # base_time를 datetime 객체로 변환 후 date 객체로 변환
                base_time_obj = datetime.strptime(base_time, "%Y-%m-%d  %H:%M").date()

                # dt_sim_mac에서 location과 동일한 mac_name을 찾고 해당하는 mac_id를 찾음
                mac_query = """
                   select sm.mac_id
                    FROM public.dt_sim_mac sm
                    left join public.tb_camera tc on tc.camera_id =  split_part(sm.mac_name , '_', 1)
                    WHERE sim_id = %s AND tc."location" = %s
                """
                cur.execute(mac_query, (sim_id, location))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                mac_rows = [dict(zip(columns, row)) for row in rows]
                print('mac_rows', mac_rows)

                if mac_rows is None:
                    # 해당 location에 맞는 mac_name이 없는 경우
                    return [{"진행": 0, "전체": 0}]

                # 진행 중인 작업 수와 전체 작업 수 계산
                in_progress = 0
                total = 0

                for mac_row in mac_rows:

                    mac_id = mac_row['mac_id']

                    # 해당 mac_id에 대한 데이터 조회
                    query = """
                        SELECT *
                        FROM public.dt_sim_list
                        WHERE sim_id = %s AND mac_id = %s
                        ORDER BY job_id
                    """
                    cur.execute(query, (sim_id, mac_id))
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    all_data = [dict(zip(columns, row)) for row in rows]

                    print('all_data', all_data)
                
                    for row in all_data:
                        # start_date가 없거나 base_time 이후인 경우는 전체수주로만 count  
                        total += 1
                        if row.get('start_date') is None or row.get('start_date') > base_time_obj:
                            continue

                        # base_time가 start_date와 end_date 사이인 경우 진행으로 count
                        if row.get('start_date') <= base_time_obj and (row.get('end_date') is None or row.get('end_date') >= base_time_obj):
                            in_progress += 1
                        elif  row.get('due_date') < base_time_obj:
                            total -= 1


                # 결과 반환
                result = [{"진행": in_progress, "전체": total}]
                return result

            # sim_id와 base_time가 모두 제공된 경우 dt_sim_list에서 데이터 조회
            elif sim_id is not None and base_time is not None:
                # base_time를 datetime 객체로 변환 후 date 객체로 변환
                base_time_obj = datetime.strptime(base_time, "%Y-%m-%d  %H:%M").date()

                # 전체 데이터 조회
                query = """
                    SELECT *
                    FROM public.dt_sim_list
                    WHERE sim_id = %s
                    ORDER BY job_id, mac_id
                """
                cur.execute(query, (sim_id,))
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                all_data = [dict(zip(columns, row)) for row in rows]

                # 진행 중인 작업 수와 전체 작업 수 계산
                in_progress = 0
                total = 0

                for row in all_data:
                            # start_date가 없거나 base_time 이후인 경우는 전체수주로만 count  
                            total += 1
                            if row.get('start_date') is None or row.get('start_date') > base_time_obj:
                                continue

                            # base_time가 start_date와 end_date 사이인 경우 진행으로 count
                            if row.get('start_date') <= base_time_obj and (row.get('end_date') is None or row.get('end_date') >= base_time_obj):
                                in_progress += 1
                            elif  row.get('due_date') < base_time_obj:
                                total -= 1


                # 결과 반환
                result = [{"진행": in_progress, "전체": total}]
                return result
            # 기존 기능 유지
            elif location == None:
                query1 = """
                    SELECT
                        COUNT(CASE WHEN sub.작업진행율 IS NOT NULL THEN 1 END) AS 진행,
                        COUNT(*) AS 전체
                    from  (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            bwts.dlvdt as "납기일자",
                            bwts.enddt as "종료일자"
                        FROM public.dt_leg_ord_data_bwts bwts
                        left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no , '-', 1) and bwts.ordseq = CAST(NULLIF(split_part(ttd.order_no , '-', 2), '')  as numeric)
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no , '-', 1) and bwts.ordseq = CAST(NULLIF(split_part(ttui.order_no , '-', 2), '') as numeric) 
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((bwts.filter_sts = 'C' or bwts.filter_sts = '_0'  ) 
                        and (bwts.skid_sts = 'C' or bwts.skid_sts = '_0'  ) 
                        and (bwts.uv_sts  = 'C' or bwts.uv_sts  = '_0'))
                        order by ordnum desc, ordseq desc) sub
                """
                # print(query)
                cur.execute(query1, ())
                rows1 = cur.fetchall()
                query2 = """
                    SELECT
                        COUNT(CASE WHEN sub.작업진행율 IS NOT NULL THEN 1 END) AS 진행,
                        COUNT(*) AS 전체
                    from  (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            scrubber.dlvdt as "납기일자",
                            scrubber.enddt as "종료일자"
                        FROM public.dt_leg_ord_data_scrubber scrubber
                        left join public.tb_twin_detection ttd on scrubber.ordnum = split_part(ttd.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no , '-', 2), '')  as numeric)
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on scrubber.ordnum = split_part(ttui.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttui.order_no , '-', 2), '') as numeric) 
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((scrubber.cutting_sts = 'C' or scrubber.cutting_sts = '_0') 
                                    and (scrubber.bending_sts = 'C' or scrubber.bending_sts = '_0') 
                                    and (scrubber.fit_wel_sts  = 'C' or scrubber.fit_wel_sts  = '_0') 
                                    and (scrubber.pt_sts  = 'C' or scrubber.pt_sts  = '_0') 
                                    and (scrubber.vl_dl_sts  = 'C' or scrubber.vl_dl_sts  = '_0') 
                                    and (scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
                                    and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
                                    and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
                                    and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0'))
                        order by ordnum desc, ordseq desc) sub
                """
                # print(query)
                cur.execute(query2, ())
                rows2 = cur.fetchall()
            else:
                query1 = """
                    SELECT
                        COUNT(CASE WHEN sub.작업진행율 IS NOT NULL THEN 1 END) AS 진행,
                        COUNT(*) AS 전체
                    from  (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            bwts.dlvdt as "납기일자",
                            bwts.enddt as "종료일자"
                        FROM public.dt_leg_ord_data_bwts bwts
                        left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no , '-', 1) and bwts.ordseq = CAST(NULLIF(split_part(ttd.order_no , '-', 2), '') as numeric)
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no , '-', 1) and bwts.ordseq = CAST(NULLIF(split_part(ttui.order_no , '-', 2), '') as numeric) 
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((bwts.filter_sts = 'C' or bwts.filter_sts = '_0'  ) 
                        and (bwts.skid_sts = 'C' or bwts.skid_sts = '_0'  ) 
                        and (bwts.uv_sts  = 'C' or bwts.uv_sts  = '_0'))
                        order by ordnum desc, ordseq desc) sub
                    where sub."위치" = %s or sub."위치" is null
                """
                # print(query)
                cur.execute(query1, (location,))
                rows1 = cur.fetchall()
                query2 = """
                    SELECT
                        COUNT(CASE WHEN sub.작업진행율 IS NOT NULL THEN 1 END) AS 진행,
                        COUNT(*) AS 전체
                    from  (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            scrubber.dlvdt as "납기일자",
                            scrubber.enddt as "종료일자"
                        FROM public.dt_leg_ord_data_scrubber scrubber
                        left join public.tb_twin_detection ttd on scrubber.ordnum = split_part(ttd.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no , '-', 2), '')  as numeric)
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on scrubber.ordnum = split_part(ttui.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttui.order_no , '-', 2), '') as numeric) 
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((scrubber.cutting_sts = 'C' or scrubber.cutting_sts = '_0') 
                                    and (scrubber.bending_sts = 'C' or scrubber.bending_sts = '_0') 
                                    and (scrubber.fit_wel_sts  = 'C' or scrubber.fit_wel_sts  = '_0') 
                                    and (scrubber.pt_sts  = 'C' or scrubber.pt_sts  = '_0') 
                                    and (scrubber.vl_dl_sts  = 'C' or scrubber.vl_dl_sts  = '_0') 
                                    and (scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
                                    and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
                                    and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
                                    and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0'))
                        order by ordnum desc, ordseq desc) sub
                    where sub."위치" = %s or sub."위치" is null
                """
                # print(query)
                cur.execute(query2, (location,))
                rows2 = cur.fetchall()
            # rows1, rows2 조회 이후
            columns = [desc[0] for desc in cur.description]   # ['진행', '전체'] 예상
            col_idx = {name: i for i, name in enumerate(columns)}

            rows_all = rows1 + rows2

            sum_progress = sum((row[col_idx["진행"]] or 0) for row in rows_all)
            sum_total    = sum((row[col_idx["전체"]] or 0) for row in rows_all)

            result = [{
                "전체": sum_total,
                "진행": sum_progress
            }]
            
            return result

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return []

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_inven(location=None, base_time=None, sim_id=None):
    """
    location기준으로 수주진행 현황 데이터를 로드
    sim_id가 제공되면 해당 시뮬레이션의 진행률을 계산
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:

            # sim_id가 제공된 경우, 진행률 계산 로직 사용(BWTS 전용)
            result0 = None
            if sim_id is not None:
                # 해당 sim_id가 bwts 또는 scrubber 인지 확인
                query = """
                            select dsm.type_code 
                            from public.dt_sim_master dsm 
                            where dsm.sim_id = %s

                """
                cur.execute(query, (sim_id,))
                row = cur.fetchone()
                type_code = row[0]
                print('type_code', type_code)

                if type_code == 'BWTS' or type_code is None:
                    if location is not None:
                        query = """
                                    select foo.progress_percentage, foo.sim_id, foo.mac_id, foo.mac_name, foo.start_date, foo.end_date, foo.due_date, tc.location
                                        from (WITH progress AS (
                                                            SELECT CASE
                                                                    WHEN TIMESTAMP %s < d.start_date THEN 0
                                                                    WHEN TIMESTAMP %s >= d.end_date THEN 100
                                                                    ELSE
                                                                        ROUND(
                                                                                ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                                                                    EXTRACT(EPOCH FROM d.start_date)) /
                                                                                    NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                                                                100
                                                                        )
                                                                    END AS exact_progress,
                                                                d.sim_id,
                                                                d.mac_id,
                                                                d.start_date,
                                                                d.end_date,
                                                                d.due_date,
                                                                m.mac_name,
                                                                split_part(m.mac_name , '_', 1) as camera_id
                                                            FROM dt_sim_list d
                                                                    LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                                                                    left join public.tb_camera tc on split_part(m.mac_name , '_', 1) = tc.camera_id
                                                            WHERE d.sim_id = %s
                                                                AND m.mac_name IS NOT NULL
                                                                AND LOWER(m.mac_name) != 'none'
                                                        )
                                                        SELECT DISTINCT ON (mac_name)
                                                            CASE
                                                                WHEN exact_progress <= 0 THEN 0
                                                                WHEN exact_progress <= 10 THEN 10
                                                                WHEN exact_progress <= 30 THEN 30
                                                                WHEN exact_progress <= 50 THEN 50
                                                                WHEN exact_progress <= 70 THEN 70
                                                                WHEN exact_progress <= 90 THEN 90
                                                                ELSE 100
                                                                END AS progress_percentage,
                                                            sim_id,
                                                            mac_id,
                                                            mac_name,
                                                            start_date,
                                                            end_date,
                                                            due_date,
                                                            camera_id
                                                        FROM progress
                                                        WHERE due_date >= TIMESTAMP %s 
                                                        ORDER BY mac_name, sim_id, mac_id) foo
                                        left join public.tb_camera tc on tc.camera_id = foo.camera_id
                                        where tc.location = %s;
                        """
                        # 현재 시간 또는 제공된 base_time 사용
                        current_time = base_time if base_time is not None else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        cur.execute(query, (current_time, current_time, current_time, sim_id, current_time, location,))

                        rows = cur.fetchall()
                        columns = [desc[0] for desc in cur.description]
                        result0 = [dict(zip(columns, row)) for row in rows]

                        # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
                        filtered_results = []

                        
                        for result in result0:
                            if result.get('mac_name'):
                                mac_name = result['mac_name']
                                # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                                match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                                if match:
                                    # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                                    temp = {}
                                    extracted_camera_id = mac_name.split('_')[0]
                                    temp['image_id'] = None
                                    temp['object_label'] = 'BWTS_' + str(result['progress_percentage']) + '%'
                                    temp['camera_id'] = extracted_camera_id
                                    temp['localtion'] = result['location']

                                    filtered_results.append(temp)
             
                        print('filtered_results', filtered_results)

                        # 검출 내역 에서 라벨명 bwts로 시작 되는 항목 제거하고 합치기
                        with conn.cursor() as detection_cur:
                            # 최신 이미지 ID 조회
                            detection_cur.execute('''
                            WITH latest_images AS (
                                SELECT sub.image_id
                                FROM (
                                    SELECT DISTINCT ON (tti.camera_id) *
                                    FROM tb_twin_image tti
                                    where tti.created_at <= %s
                                    ORDER BY tti.camera_id, tti.created_at DESC
                                ) sub
                            )
                            SELECT ttd.image_id, 
                                    CASE 
                                        WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                        THEN SPLIT_PART(ttd.object_label, '_', 1)
                                        ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                    END AS object_label
                                    , tti.camera_id, tc.location
                            FROM tb_twin_detection ttd
                            left join tb_twin_image tti on ttd.image_id = tti.image_id
                            left join tb_camera tc on tc.camera_id = tti.camera_id
                            WHERE ttd.image_id IN (SELECT image_id FROM latest_images) and tc.location = %s
                            ''', (current_time, location, ))

                            # 결과 가져오기
                            detection_rows = detection_cur.fetchall()
                            detection_col_names = [desc[0] for desc in detection_cur.description]

                            print('detection_col_names', detection_col_names)

                            # 데이터 변환
                            detection_data = [
                                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                                for row in detection_rows
                            ]

                            # "BWTS_" 로 시작하는 object_label 제거
                            detection_data = [d for d in detection_data if not d["object_label"].startswith("BWTS_")]
                            print('detection_data', detection_data)

                            if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                                detection_data = []

                        all_data = filtered_results + detection_data 

                        result1 = {}
                        # 총 데이터 수량
                        result1['총수량'] = len(all_data)

                        # object_label의 고유 항목 수
                        result1['품목수'] = len(set(d['object_label'] for d in all_data))

                        return {"data": result1}
                    else:
                        query = """
                                    select foo.progress_percentage, foo.sim_id, foo.mac_id, foo.mac_name, foo.start_date, foo.end_date, foo.due_date, tc.location
                                        from (WITH progress AS (
                                                            SELECT CASE
                                                                    WHEN TIMESTAMP %s < d.start_date THEN 0
                                                                    WHEN TIMESTAMP %s >= d.end_date THEN 100
                                                                    ELSE
                                                                        ROUND(
                                                                                ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                                                                    EXTRACT(EPOCH FROM d.start_date)) /
                                                                                    NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                                                                100
                                                                        )
                                                                    END AS exact_progress,
                                                                d.sim_id,
                                                                d.mac_id,
                                                                d.start_date,
                                                                d.end_date,
                                                                d.due_date,
                                                                m.mac_name,
                                                                split_part(m.mac_name , '_', 1) as camera_id
                                                            FROM dt_sim_list d
                                                                    LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                                                                    left join public.tb_camera tc on split_part(m.mac_name , '_', 1) = tc.camera_id
                                                            WHERE d.sim_id = %s
                                                                AND m.mac_name IS NOT NULL
                                                                AND LOWER(m.mac_name) != 'none'
                                                        )
                                                        SELECT DISTINCT ON (mac_name)
                                                            CASE
                                                                WHEN exact_progress <= 0 THEN 0
                                                                WHEN exact_progress <= 10 THEN 10
                                                                WHEN exact_progress <= 30 THEN 30
                                                                WHEN exact_progress <= 50 THEN 50
                                                                WHEN exact_progress <= 70 THEN 70
                                                                WHEN exact_progress <= 90 THEN 90
                                                                ELSE 100
                                                                END AS progress_percentage,
                                                            sim_id,
                                                            mac_id,
                                                            mac_name,
                                                            start_date,
                                                            end_date,
                                                            due_date,
                                                            camera_id
                                                        FROM progress
                                                        WHERE due_date >= TIMESTAMP %s 
                                                        ORDER BY mac_name, sim_id, mac_id) foo
                                        left join public.tb_camera tc on tc.camera_id = foo.camera_id;
                        """
                        # 현재 시간 또는 제공된 base_time 사용
                        current_time = base_time if base_time is not None else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        cur.execute(query, (current_time, current_time, current_time, sim_id, current_time,))

                        rows = cur.fetchall()
                        columns = [desc[0] for desc in cur.description]
                        result0 = [dict(zip(columns, row)) for row in rows]

                        # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
                        filtered_results = []

                        
                        for result in result0:
                            if result.get('mac_name'):
                                mac_name = result['mac_name']
                                # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                                match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                                if match:
                                    # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                                    temp = {}
                                    extracted_camera_id = mac_name.split('_')[0]
                                    temp['image_id'] = None
                                    temp['object_label'] = 'BWTS_' + str(result['progress_percentage']) + '%'
                                    temp['camera_id'] = extracted_camera_id
                                    temp['localtion'] = result['location']

                                    filtered_results.append(temp)
             
                        print('filtered_results', filtered_results)

                        # 검출 내역 에서 라벨명 bwts로 시작 되는 항목 제거하고 합치기
                        with conn.cursor() as detection_cur:
                            # 최신 이미지 ID 조회
                            detection_cur.execute('''
                            WITH latest_images AS (
                                SELECT sub.image_id
                                FROM (
                                    SELECT DISTINCT ON (tti.camera_id) *
                                    FROM tb_twin_image tti
                                    where tti.created_at <= %s
                                    ORDER BY tti.camera_id, tti.created_at DESC
                                ) sub
                            )
                            SELECT ttd.image_id, 
                                    CASE 
                                        WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                        THEN SPLIT_PART(ttd.object_label, '_', 1)
                                        ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                    END AS object_label
                                    , tti.camera_id, tc.location
                            FROM tb_twin_detection ttd
                            left join tb_twin_image tti on ttd.image_id = tti.image_id
                            left join tb_camera tc on tc.camera_id = tti.camera_id
                            WHERE ttd.image_id IN (SELECT image_id FROM latest_images)
                            ''', (current_time, ))

                            # 결과 가져오기
                            detection_rows = detection_cur.fetchall()
                            detection_col_names = [desc[0] for desc in detection_cur.description]

                            print('detection_col_names', detection_col_names)

                            # 데이터 변환
                            detection_data = [
                                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                                for row in detection_rows
                            ]

                            # "BWTS_" 로 시작하는 object_label 제거
                            detection_data = [d for d in detection_data if not d["object_label"].startswith("BWTS_")]
                            print('detection_data', detection_data)

                            if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                                detection_data = []

                        all_data = filtered_results + detection_data 

                        result1 = {}
                        # 총 데이터 수량
                        result1['총수량'] = len(all_data)

                        # object_label의 고유 항목 수
                        result1['품목수'] = len(set(d['object_label'] for d in all_data))

                        return {"data": result1}


            # 기존 로직 (sim_id가 제공되지 않은 경우)
            if base_time == None:
                if location == None:
                    query = """
                        SELECT 
                            COUNT(sub_f.object_label) AS 총수량,
                            COUNT(DISTINCT sub_f.object_label) AS 품목수                   
                        from (WITH latest_images AS (
                                        SELECT sub.image_id
                                        FROM (
                                            SELECT DISTINCT ON (tti.camera_id) *
                                            FROM tb_twin_image tti
                                            ORDER BY tti.camera_id, tti.created_at DESC
                                        ) sub
                                    )
                                    SELECT ttd.image_id, 
                                            CASE 
                                                WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                                THEN SPLIT_PART(ttd.object_label, '_', 1)
                                                ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                            END AS object_label
                                            , tti.camera_id, tc.location
                                    FROM tb_twin_detection ttd
                                    left join tb_twin_image tti on ttd.image_id = tti.image_id
                                    left join tb_camera tc on tc.camera_id = tti.camera_id
                                    WHERE ttd.image_id IN (SELECT image_id FROM latest_images)
                        ) sub_f
                    """
                    # print(query)
                    cur.execute(query, ())
                else:
                    query = """
                        SELECT 
                            COUNT(sub_f.object_label) AS 총수량,
                            COUNT(DISTINCT sub_f.object_label) AS 품목수                   
                        from (WITH latest_images AS (
                                        SELECT sub.image_id
                                        FROM (
                                            SELECT DISTINCT ON (tti.camera_id) *
                                            FROM tb_twin_image tti
                                            ORDER BY tti.camera_id, tti.created_at DESC
                                        ) sub
                                    )
                                    SELECT ttd.image_id, 
                                            CASE 
                                                WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                                THEN SPLIT_PART(ttd.object_label, '_', 1)
                                                ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                            END AS object_label
                                            , tti.camera_id, tc.location
                                    FROM tb_twin_detection ttd
                                    left join tb_twin_image tti on ttd.image_id = tti.image_id
                                    left join tb_camera tc on tc.camera_id = tti.camera_id
                                    WHERE ttd.image_id IN (SELECT image_id FROM latest_images) and tc.location = %s 
                        ) sub_f
                    """
                    # print(query)
                    cur.execute(query, (location, ))
            else:
                if location == None:
                    query = """
                        SELECT 
                            COUNT(sub_f.object_label) AS 총수량,
                            COUNT(DISTINCT sub_f.object_label) AS 품목수                   
                        from (WITH latest_images AS (
                                        SELECT sub.image_id
                                        FROM (
                                            SELECT DISTINCT ON (tti.camera_id) *
                                            FROM tb_twin_image tti
                                            where tti.created_at <= %s
                                            ORDER BY tti.camera_id, tti.created_at DESC
                                        ) sub
                                    )
                                    SELECT ttd.image_id, 
                                            CASE 
                                                WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                                THEN SPLIT_PART(ttd.object_label, '_', 1)
                                                ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                            END AS object_label
                                            , tti.camera_id, tc.location
                                    FROM tb_twin_detection ttd
                                    left join tb_twin_image tti on ttd.image_id = tti.image_id
                                    left join tb_camera tc on tc.camera_id = tti.camera_id
                                    WHERE ttd.image_id IN (SELECT image_id FROM latest_images)
                        ) sub_f
                    """
                    # print(query)
                    cur.execute(query, (base_time,))
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    result1 = [dict(zip(columns, row)) for row in rows]
                else:
                    query = """
                        SELECT 
                            COUNT(sub_f.object_label) AS 총수량,
                            COUNT(DISTINCT sub_f.object_label) AS 품목수                   
                        from (WITH latest_images AS (
                                        SELECT sub.image_id
                                        FROM (
                                            SELECT DISTINCT ON (tti.camera_id) *
                                            FROM tb_twin_image tti
                                            where tti.created_at <= %s
                                            ORDER BY tti.camera_id, tti.created_at DESC
                                        ) sub
                                    )
                                    SELECT ttd.image_id, 
                                            CASE 
                                                WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                                THEN SPLIT_PART(ttd.object_label, '_', 1)
                                                ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                                            END AS object_label
                                            , tti.camera_id, tc.location
                                    FROM tb_twin_detection ttd
                                    left join tb_twin_image tti on ttd.image_id = tti.image_id
                                    left join tb_camera tc on tc.camera_id = tti.camera_id
                                    WHERE ttd.image_id IN (SELECT image_id FROM latest_images) and tc.location = %s 
                        ) sub_f
                    """
                    # print(query)
                    cur.execute(query, (base_time, location, ))
                    rows = cur.fetchall()
                    columns = [desc[0] for desc in cur.description]
                    result1 = [dict(zip(columns, row)) for row in rows]

            # If sim_id is provided, count the number of unique progress_percentage values
            # if sim_id is not None:
            #     merged = []
            #     for d0, d1 in zip(result0, result1):
            #         merged.append({k: d0[k] + d1[k] for k in d0})

            #     return {"data": merged}
            

            return {"data": result1}
           

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return {"data": [{ "총수량": 0, "품목수": 0}]}

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


def fn_get_sim_input_bwts(base_time: str, block_ids = None):
    """
    base_time기준으로 수주진행 현황 데이터를 로드
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if block_ids == None:
                # query = """
                #     SELECT f.수주번호,
                #         f.순번,
                #         f.작업진행율,
                #         (SELECT tc.location
                #             FROM public.tb_camera tc
                #             WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                #             LIMIT 1)    as "카메라위치",
                #         f.위치,
                #         f.상세위치,
                #         f.납기일자,
                #         f.종료일자,
                #         dlodb.hullno as "호선",
                #         camera.camera_desc as "카메라위치",
                #         f.납기초과여부
                #     from (SELECT distinct on (ordnum, ordseq) ordnum     as "수주번호",
                #                                             ordseq     as "순번",
                #                                             case
                #                                                 when ttd.object_label is NOT null
                #                                                     then split_part(ttd.object_label, '_', 2)
                #                                                 when ttui.object_label is NOT null
                #                                                     then split_part(ttui.object_label, '_', 2)
                #                                                 end    as "작업진행율",
                #                                             case
                #                                                 when tc.location is NOT null then tc.location
                #                                                 when tc1.location is NOT null then tc1.location
                #                                                 end    as "위치",
                #                                             case
                #                                                 when tti.camera_id is NOT null then
                #                                                     tti.camera_id || '_R' || ttd.detected_row || 'C' ||
                #                                                     ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                #                                                 when tti1.camera_id is NOT null then
                #                                                     tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col ||
                #                                                     '_' || ttui.grid_width || 'x' || ttui.grid_height
                #                                                 end    as "상세위치",
                #                                             bwts.dlvdt as "납기일자",
                #                                             bwts.enddt as "종료일자",
                #                                             CASE
                #                                                 WHEN bwts.dlvdt::text ~ '^\d{8}$'
                #                                                     AND to_date(bwts.dlvdt::text, 'YYYYMMDD') < %s::date
                #                                                     THEN TRUE ELSE FALSE
                #                                                 END AS "납기초과여부"
                #         FROM public.dt_leg_ord_data_bwts bwts
                #                 left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no, '-', 1) and
                #                                                             bwts.ordseq =
                #                                                             CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and
                #                                                             ttd.created_at <= %s
                #                 left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                #                 left join public.tb_camera tc on tti.camera_id = tc.camera_id
                #                 left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no, '-', 1) and
                #                                                             bwts.ordseq =
                #                                                             CAST(NULLIF(split_part(ttui.order_no, '-', 2), '') as numeric) and
                #                                                             ttui.created_at <= %s
                #                 left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                #                 left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                #         where not ((bwts.filter_sts = 'C' or bwts.filter_sts = '_0')
                #             and (bwts.skid_sts = 'C' or bwts.skid_sts = '_0')
                #             and (bwts.uv_sts = 'C' or bwts.uv_sts = '_0'))
                #         order by ordnum desc, ordseq desc) AS f
                #             left join public.dt_leg_ord_data_bwts as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                #             LEFT JOIN public.tb_camera AS camera
                #                     ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1)

                # """
                
                query = """
                    SELECT f.수주번호,
                        f.순번,
                        f.작업진행율,
                        (SELECT tc.location
                            FROM public.tb_camera tc
                            WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                            LIMIT 1)    as "카메라위치",
                        f.위치,
                        f.상세위치,
                        f.납기일자,
                        f.종료일자,
                        dlodb.hullno as "호선",
                        camera.camera_desc as "카메라위치",
                        f.납기초과여부
                    from (SELECT distinct on (ordnum, ordseq) ordnum     as "수주번호",
                                                            ordseq     as "순번",
                                                            case
                                                                when ttd.object_label is NOT null
                                                                    then split_part(ttd.object_label, '_', 2)
                                                                when ttui.object_label is NOT null
                                                                    then split_part(ttui.object_label, '_', 2)
                                                                end    as "작업진행율",
                                                            case
                                                                when tc.location is NOT null then tc.location
                                                                when tc1.location is NOT null then tc1.location
                                                                end    as "위치",
                                                            case
                                                                when tti.camera_id is NOT null then
                                                                    tti.camera_id || '_R' || ttd.detected_row || 'C' ||
                                                                    ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                                                when tti1.camera_id is NOT null then
                                                                    tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col ||
                                                                    '_' || ttui.grid_width || 'x' || ttui.grid_height
                                                                end    as "상세위치",
                                                            bwts.dlvdt as "납기일자",
                                                            bwts.enddt as "종료일자",
                                                            CASE
                                                                WHEN bwts.dlvdt::text ~ '^\d{8}$'
                                                                    AND to_date(bwts.dlvdt::text, 'YYYYMMDD') < %s::date
                                                                    THEN TRUE ELSE FALSE
                                                                END AS "납기초과여부"
                        FROM public.dt_leg_ord_data_bwts bwts
                                left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no, '-', 1) and
                                                                            bwts.ordseq =
                                                                            CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and
                                                                            ttd.created_at <= %s
                                left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                                left join public.tb_camera tc on tti.camera_id = tc.camera_id
                                left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no, '-', 1) and
                                                                            bwts.ordseq =
                                                                            CAST(NULLIF(split_part(ttui.order_no, '-', 2), '') as numeric) and
                                                                            ttui.created_at <= %s
                                left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                                left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not (bwts.skid_sts = 'C' or bwts.skid_sts = '_0') and to_date(bwts.dlvdt::text, 'YYYYMMDD') > %s::date
                        order by ordnum desc, ordseq desc) AS f
                            left join public.dt_leg_ord_data_bwts as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                            LEFT JOIN public.tb_camera AS camera
                                    ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1)

                """
                # print(query)
                cur.execute(query, (base_time, base_time, base_time, base_time))
            else:
                # query = """
                #     SELECT f.수주번호,
                #         f.순번,
                #         f.작업진행율,
                #         (SELECT tc.location
                #             FROM public.tb_camera tc
                #             WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                #             LIMIT 1)    as "카메라위치",
                #         f.위치,
                #         f.상세위치,
                #         f.납기일자,
                #         f.종료일자,
                #         dlodb.hullno as "호선",
                #         camera.camera_desc as "카메라위치",
                #         f.납기초과여부
                #     from (SELECT distinct on (ordnum, ordseq) ordnum     as "수주번호",
                #                                             ordseq     as "순번",
                #                                             case
                #                                                 when ttd.object_label is NOT null
                #                                                     then split_part(ttd.object_label, '_', 2)
                #                                                 when ttui.object_label is NOT null
                #                                                     then split_part(ttui.object_label, '_', 2)
                #                                                 end    as "작업진행율",
                #                                             case
                #                                                 when tc.location is NOT null then tc.location
                #                                                 when tc1.location is NOT null then tc1.location
                #                                                 end    as "위치",
                #                                             case
                #                                                 when tti.camera_id is NOT null then
                #                                                     tti.camera_id || '_R' || ttd.detected_row || 'C' ||
                #                                                     ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                #                                                 when tti1.camera_id is NOT null then
                #                                                     tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col ||
                #                                                     '_' || ttui.grid_width || 'x' || ttui.grid_height
                #                                                 end    as "상세위치",
                #                                             bwts.dlvdt as "납기일자",
                #                                             bwts.enddt as "종료일자",
                #                                             CASE
                #                                                 WHEN bwts.dlvdt::text ~ '^\d{8}$'
                #                                                     AND to_date(bwts.dlvdt::text, 'YYYYMMDD') < %s::date
                #                                                     THEN TRUE ELSE FALSE
                #                                                 END AS "납기초과여부"
                #         FROM public.dt_leg_ord_data_bwts bwts
                #                 left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no, '-', 1) and
                #                                                             bwts.ordseq =
                #                                                             CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and
                #                                                             ttd.created_at <= %s
                #                 left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                #                 left join public.tb_camera tc on tti.camera_id = tc.camera_id
                #                 left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no, '-', 1) and
                #                                                             bwts.ordseq =
                #                                                             CAST(NULLIF(split_part(ttui.order_no, '-', 2), '') as numeric) and
                #                                                             ttui.created_at <= %s
                #                 left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                #                 left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                #         where not ((bwts.filter_sts = 'C' or bwts.filter_sts = '_0')
                #             and (bwts.skid_sts = 'C' or bwts.skid_sts = '_0')
                #             and (bwts.uv_sts = 'C' or bwts.uv_sts = '_0'))
                #         order by ordnum desc, ordseq desc) AS f
                #             left join public.dt_leg_ord_data_bwts as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                #             LEFT JOIN public.tb_camera AS camera
                #                     ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                #             where  concat_ws('-', f.수주번호, f.순번::text) <> ALL(%s)
                query = """
                    SELECT f.수주번호,
                        f.순번,
                        f.작업진행율,
                        (SELECT tc.location
                            FROM public.tb_camera tc
                            WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                            LIMIT 1)    as "카메라위치",
                        f.위치,
                        f.상세위치,
                        f.납기일자,
                        f.종료일자,
                        dlodb.hullno as "호선",
                        camera.camera_desc as "카메라위치",
                        f.납기초과여부
                    from (SELECT distinct on (ordnum, ordseq) ordnum     as "수주번호",
                                                            ordseq     as "순번",
                                                            case
                                                                when ttd.object_label is NOT null
                                                                    then split_part(ttd.object_label, '_', 2)
                                                                when ttui.object_label is NOT null
                                                                    then split_part(ttui.object_label, '_', 2)
                                                                end    as "작업진행율",
                                                            case
                                                                when tc.location is NOT null then tc.location
                                                                when tc1.location is NOT null then tc1.location
                                                                end    as "위치",
                                                            case
                                                                when tti.camera_id is NOT null then
                                                                    tti.camera_id || '_R' || ttd.detected_row || 'C' ||
                                                                    ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                                                when tti1.camera_id is NOT null then
                                                                    tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col ||
                                                                    '_' || ttui.grid_width || 'x' || ttui.grid_height
                                                                end    as "상세위치",
                                                            bwts.dlvdt as "납기일자",
                                                            bwts.enddt as "종료일자",
                                                            CASE
                                                                WHEN bwts.dlvdt::text ~ '^\d{8}$'
                                                                    AND to_date(bwts.dlvdt::text, 'YYYYMMDD') < %s::date
                                                                    THEN TRUE ELSE FALSE
                                                                END AS "납기초과여부"
                        FROM public.dt_leg_ord_data_bwts bwts
                                left join public.tb_twin_detection ttd on bwts.ordnum = split_part(ttd.order_no, '-', 1) and
                                                                            bwts.ordseq =
                                                                            CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and
                                                                            ttd.created_at <= %s
                                left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                                left join public.tb_camera tc on tti.camera_id = tc.camera_id
                                left join public.tb_twin_user_input ttui on bwts.ordnum = split_part(ttui.order_no, '-', 1) and
                                                                            bwts.ordseq =
                                                                            CAST(NULLIF(split_part(ttui.order_no, '-', 2), '') as numeric) and
                                                                            ttui.created_at <= %s
                                left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                                left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not (bwts.skid_sts = 'C' or bwts.skid_sts = '_0') and to_date(bwts.dlvdt::text, 'YYYYMMDD') > %s::date
                        order by ordnum desc, ordseq desc) AS f
                            left join public.dt_leg_ord_data_bwts as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                            LEFT JOIN public.tb_camera AS camera
                                    ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                            where  concat_ws('-', f.수주번호, f.순번::text) <> ALL(%s)

                """
                # print(query)
                cur.execute(query, (base_time, base_time, base_time, base_time, block_ids))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            return result

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return []

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_run_sim_input(sim_id: str):
    """
    base_time기준으로 수주진행 현황 데이터를 로드
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            query = """
              SELECT sim_id as "sim_id",
                        ordnum as "수주번호", 
                        ordseq as "수주순번",
                        hullno as "호선",
                        rate as "진행율",
                        "location" as "공장",
                        detail_location as "작업위치",
                        dlvdt as "납기일자",
                        enddt as  "종료일자"
                FROM public.dt_sim_input
                where sim_id = %s;

            """
            # print(query)
            cur.execute(query, (sim_id,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            return result

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return []

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def fn_get_sim_input_scrubber(base_time: str, block_ids = None):
    """
    base_time기준으로 수주진행 현황 데이터를 로드
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            if block_ids == None:
                query = """
                    SELECT f.수주번호,
                        f.순번,
                        f.작업진행율,
                        (SELECT tc.location
                            FROM public.tb_camera tc
                            WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                            LIMIT 1)    as "카메라위치",
                        f.위치,
                        f.상세위치,
                        f.납기일자,
                        f.종료일자,
                        dlodb.hullno as "호선",
                        camera.camera_desc as "카메라위치",
                        f.납기초과여부
                    from (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            scrubber.dlvdt as "납기일자",
                            scrubber.enddt as "종료일자",
                            CASE
                                WHEN scrubber.dlvdt::text ~ '^\d{8}$'
                                    AND to_date(scrubber.dlvdt::text, 'YYYYMMDD') < %s::date
                                THEN TRUE ELSE FALSE
                            END AS "납기초과여부"
                        FROM public.dt_leg_ord_data_scrubber scrubber
                        left join public.tb_twin_detection ttd on scrubber.ordnum = split_part(ttd.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and ttd.created_at <= %s
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on scrubber.ordnum = split_part(ttui.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and ttui.created_at <= %s
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((scrubber.cutting_sts = 'C' or scrubber.cutting_sts = '_0') 
                            and (scrubber.bending_sts = 'C' or scrubber.bending_sts = '_0') 
                            and (scrubber.fit_wel_sts  = 'C' or scrubber.fit_wel_sts  = '_0') 
                            and (scrubber.pt_sts  = 'C' or scrubber.pt_sts  = '_0') 
                            and (scrubber.vl_dl_sts  = 'C' or scrubber.vl_dl_sts  = '_0') 
                            and (scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
                            and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
                            and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
                            and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0')
                            ) AND to_date(scrubber.dlvdt::text, 'YYYYMMDD') > %s::date
                        order by ordnum desc, ordseq DESC) AS f
                    left join public.dt_leg_ord_data_scrubber as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                            LEFT JOIN public.tb_camera AS camera
                                    ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1) 

                """
                cur.execute(query, (base_time, base_time, base_time, base_time))
            else:
                query = """
                    SELECT f.수주번호,
                        f.순번,
                        f.작업진행율,
                        (SELECT tc.location
                            FROM public.tb_camera tc
                            WHERE tc.camera_id = SPLIT_PART(f.상세위치, '_', 1)
                            LIMIT 1)    as "카메라위치",
                        f.위치,
                        f.상세위치,
                        f.납기일자,
                        f.종료일자,
                        dlodb.hullno as "호선",
                        camera.camera_desc as "카메라위치",
                        f.납기초과여부
                    from (SELECT distinct on (ordnum, ordseq)
                            ordnum as "수주번호", 
                            ordseq as "순번", 
                            case 
                                when ttd.object_label is NOT null then split_part(ttd.object_label , '_', 2)
                                when ttui.object_label is NOT null then split_part(ttui.object_label , '_', 2)
                            end as "작업진행율",
                            case 
                                when tc.location is NOT null then tc.location
                                when tc1.location is NOT null then tc1.location
                            end as "위치",
                            case 
                                when tti.camera_id  is NOT null then tti.camera_id || '_R' || ttd.detected_row || 'C' || ttd.detected_col || '_' || ttd.grid_width || 'x' || ttd.grid_height
                                when tti1.camera_id is NOT null then tti1.camera_id || '_R' || ttui.input_row || 'C' || ttui.input_col || '_' || ttui.grid_width || 'x' || ttui.grid_height
                            end as "상세위치",
                            scrubber.dlvdt as "납기일자",
                            scrubber.enddt as "종료일자",
                            CASE
                                WHEN scrubber.dlvdt::text ~ '^\d{8}$'
                                    AND to_date(scrubber.dlvdt::text, 'YYYYMMDD') < %s::date
                                THEN TRUE ELSE FALSE
                            END AS "납기초과여부"
                        FROM public.dt_leg_ord_data_scrubber scrubber
                        left join public.tb_twin_detection ttd on scrubber.ordnum = split_part(ttd.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and ttd.created_at <= %s
                        left join public.tb_twin_image tti on ttd.image_id = tti.image_id
                        left join public.tb_camera tc on tti.camera_id = tc.camera_id
                        left join public.tb_twin_user_input ttui on scrubber.ordnum = split_part(ttui.order_no , '-', 1) and scrubber.ordseq = CAST(NULLIF(split_part(ttd.order_no, '-', 2), '') as numeric) and ttui.created_at <= %s
                        left join public.tb_twin_image tti1 on ttui.image_id = tti1.image_id
                        left join public.tb_camera tc1 on tti1.camera_id = tc1.camera_id
                        where not ((scrubber.cutting_sts = 'C' or scrubber.cutting_sts = '_0') 
                            and (scrubber.bending_sts = 'C' or scrubber.bending_sts = '_0') 
                            and (scrubber.fit_wel_sts  = 'C' or scrubber.fit_wel_sts  = '_0') 
                            and (scrubber.pt_sts  = 'C' or scrubber.pt_sts  = '_0') 
                            and (scrubber.vl_dl_sts  = 'C' or scrubber.vl_dl_sts  = '_0') 
                            and (scrubber.acid_sts  = 'C' or scrubber.acid_sts  = '_0') 
                            and (scrubber.ass_sts  = 'C' or scrubber.ass_sts  = '_0') 
                            and (scrubber.insp_sts  = 'C' or scrubber.insp_sts  = '_0') 
                            and (scrubber.pack_sts  = 'C' or scrubber.pack_sts  = '_0')
                            ) AND to_date(scrubber.dlvdt::text, 'YYYYMMDD') > %s::date
                        order by ordnum desc, ordseq DESC) AS f
                    left join public.dt_leg_ord_data_scrubber as dlodb on f.수주번호 = dlodb.ordnum and f.순번 = dlodb.ordseq
                            LEFT JOIN public.tb_camera AS camera
                                    ON camera.camera_id = SPLIT_PART(f.상세위치, '_', 1) 
                            where  concat_ws('-', f.수주번호, f.순번::text) <> ALL(%s)

                """
                cur.execute(query, (base_time, base_time, base_time, base_time, block_ids))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            result = [dict(zip(columns, row)) for row in rows]
            return result

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return []

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


def insert_sim_macs(sim_id: int, mac_names: list):
    """
    sim_id와 mac_names 리스트를 받아 dt_sim_mac 테이블에 삽입하는 함수입니다.
    각 mac_name은 mac_id와 함께 삽입되며, mac_id는 0부터 시작하는 인덱스입니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO public.dt_sim_mac (sim_id, mac_id, mac_name)
                VALUES (%s, %s, %s)
            """

            for mac_id, mac_name in enumerate(mac_names):
                cur.execute(insert_query, (sim_id, mac_id, mac_name))

            conn.commit()
            print(f"✅ dt_sim_mac에 {len(mac_names)}건 입력 완료 (sim_id={sim_id})")

    except Exception as e:
        if conn:
            conn.rollback()
        print("❌ 오류 발생:", e)

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


def insert_sim_jobs(sim_id: int, job_names: list):
    """
    sim_id와 job_names 리스트를 받아 dt_sim_job 테이블에 삽입하는 함수입니다.
    각 job_name은 job_id와 함께 삽입되며, job_id는 0부터 시작하는 인덱스입니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO public.dt_sim_job (sim_id, job_id, job_name)
                VALUES (%s, %s, %s)
            """

            for job_id, job_name in enumerate(job_names):
                cur.execute(insert_query, (sim_id, job_id, job_name))

            conn.commit()
            print(f"✅ dt_sim_job에 {len(job_names)}건 입력 완료 (sim_id={sim_id})")

    except Exception as e:
        if conn:
            conn.rollback()
        print("❌ 오류 발생:", e)

    finally:
        if conn:
            cur.close()
            conn.close()

def insert_sim_input(sim_id: int, df: pd.DataFrame):
    """
    dt_sim_input 테이블에 주어진 DataFrame의 데이터를 sim_id와 함께 일괄 삽입합니다.

    필요한 컬럼: ordnum, ordseq, rate, "location", detail_location, dlvdt, enddt, hullno, work_time, free_time
    """
    # print('insert_sim_input')
    # print(df)
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO public.dt_sim_input
                (sim_id, ordnum, ordseq, rate, "location", detail_location, dlvdt, enddt, hullno, work_time, free_time)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # 반복문을 통한 행 단위 INSERT
            for _, row in df.iterrows():
                cur.execute(insert_query, (
                    sim_id,
                    row["수주번호"],
                    row["수주순번"],

                    row["작업진행률(%)"],
                    row["위치"],
                    row["작업장"],
                    row["납기일"],
                    row["종료일자"],
                    row["호선명"],
                    row["작업시간"],
                    row["여유시간"]
                ))

            conn.commit()
            print(f"✅ dt_sim_input {len(df)}건 입력 완료 (sim_id={sim_id})")

    except Exception as e:
        conn.rollback()
        print("❌ 오류 발생:", e)

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def insert_sim_list(sim_id: int, df: pd.DataFrame):
    """
    dt_sim_list 테이블에 주어진 DataFrame의 데이터를 sim_id와 함께 일괄 삽입합니다.

    필요한 컬럼: Job, Machine, Start, End, Ready Time, Due Date
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO public.dt_sim_list (
                    sim_id, job_id, mac_id,
                    start, "end", ready, due,
                    start_date, end_date, ready_date,
                    due_date, "order", machine_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            # 반복문을 통한 행 단위 INSERT
            for _, row in df.iterrows():
                cur.execute(insert_query, (
                    sim_id,
                    int(row["Job"]),
                    int(row["Machine"]),
                    int(row["Start"]),
                    int(row["End"]),
                    int(row["Ready Time"]),
                    int(row["Due Date"]),
                    row["Start Date"],
                    row["End Date"],
                    row["Ready Date"],
                    row["Due Date Date"],
                    int(row["Order"]),
                    int(row["Machine ID"])
                ))

            conn.commit()
            print(f"✅ dt_sim_list에 {len(df)}건 입력 완료 (sim_id={sim_id})")

    except Exception as e:
        conn.rollback()
        print("❌ 오류 발생:", e)

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def insert_sim_master(free_mac, relaxed_time, created_at, created_by, type_code):
    """
    dt_sim_master 테이블에 새로운 시뮬레이션 정보를 입력합니다.
    - 마지막 sim_id를 조회해 +1 한 후 사용
    - created_by는 자동으로 현재 시각 (now()) 입력
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 1. 마지막 sim_id 조회
            cur.execute("SELECT COALESCE(MAX(sim_id), 0) FROM public.dt_sim_master;")
            last_sim_id = cur.fetchone()[0]
            new_sim_id = last_sim_id + 1

            # 2. INSERT 실행
            insert_query = """
                INSERT INTO public.dt_sim_master (sim_id, free_mac, relaxed_time, created_at, created_by, type_code)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            cur.execute(insert_query, (new_sim_id, free_mac, relaxed_time, created_at, created_by, type_code))
            conn.commit()

            print(f"✅ 새로운 sim_id {new_sim_id}로 입력 완료.")
            return new_sim_id

    except Exception as e:
        print("❌ 오류 발생:", e)
        if conn:
            conn.rollback()
        return None

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def insert_sim_master_initial(created_at, created_by):
    """
    dt_sim_master 테이블에 새로운 시뮬레이션 정보를 입력합니다.
    free_mac과 relaxed_time은 빈 값으로 생성합니다.
    - 마지막 sim_id를 조회해 +1 한 후 사용
    - created_by는 자동으로 현재 시각 (now()) 입력
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 1. 마지막 sim_id 조회
            cur.execute("SELECT COALESCE(MAX(sim_id), 0) FROM public.dt_sim_master;")
            last_sim_id = cur.fetchone()[0]
            new_sim_id = last_sim_id + 1

            # 2. INSERT 실행 - free_mac과 relaxed_time은 NULL로 설정
            insert_query = """
                INSERT INTO public.dt_sim_master (sim_id, free_mac, relaxed_time, created_at, created_by)
                VALUES (%s, NULL, NULL, %s, %s);
            """
            cur.execute(insert_query, (new_sim_id, created_at, created_by))
            conn.commit()

            print(f"✅ 새로운 sim_id {new_sim_id}로 입력 완료 (free_mac, relaxed_time은 빈 값).")
            return new_sim_id

    except Exception as e:
        print("❌ 오류 발생:", e)
        if conn:
            conn.rollback()
        return None

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def update_sim_master(sim_id, free_mac, relaxed_time, status=None):
    """
    dt_sim_master 테이블의 기존 시뮬레이션 정보를 업데이트합니다.
    free_mac, relaxed_time, status 값을 업데이트합니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # UPDATE 실행
            if status is not None:
                update_query = """
                    UPDATE public.dt_sim_master 
                    SET free_mac = %s, relaxed_time = %s, sim_process = %s
                    WHERE sim_id = %s;
                """
                cur.execute(update_query, (free_mac, relaxed_time, status, sim_id))
            else:
                update_query = """
                    UPDATE public.dt_sim_master 
                    SET free_mac = %s, relaxed_time = %s
                    WHERE sim_id = %s;
                """
                cur.execute(update_query, (free_mac, relaxed_time, sim_id))
            affected_rows = cur.rowcount
            conn.commit()

            if affected_rows > 0:
                print(f"✅ sim_id {sim_id}의 free_mac, relaxsed_time, status 업데이트 완료.")
                return True
            else:
                print(f"❌ sim_id {sim_id}를 찾을 수 없습니다.")
                return False

    except Exception as e:
        print("❌ 오류 발생:", e)
        if conn:
            conn.rollback()
        return False

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")




def get_sim_input(sim_id):
    """
    sim_id에 해당하는 시뮬레이션 입력값을 조회하여 반환
    """
    try:
        # 마스터 정보 조회
        conn = get_connection()
        master_query = """
            SELECT * FROM public.dt_sim_master
            WHERE sim_id = %s
        """
        master_df = pd.read_sql(master_query, conn, params=(sim_id,))

        if master_df.empty:
            return None

        # 입력 데이터 조회
        input_df = get_sim_input_df(sim_id)

        # 작업 정보 조회
        job_df = get_sim_job_df(sim_id)

        # 기계 정보 조회
        mac_df = get_sim_mac_df(sim_id)

        # 결과 구성
        result = {
            'master': master_df.iloc[0].to_dict(),
            'input': input_df.to_dict('records') if not input_df.empty else [],
            'jobs': job_df.to_dict('records') if not job_df.empty else [],
            'machines': mac_df.to_dict('records') if not mac_df.empty else []
        }

        return result
    except Exception as e:
        print("❌ 시뮬레이션 입력값 조회 중 오류 발생:", e)
        return None
    finally:
        if conn:
            conn.close()
        print("Closed to PostgreSQL successfully.")

def sim_run_df(job_list, machine_list, created_at, created_by, type_code):
    """ 시뮬레이션을 실행하고 결과를 db에 저장하고 해당 결과의 sim_id를 반환 """



    #sim_master 내용 추가
    sim_id = insert_sim_master(free_mac=None, relaxed_time=None, created_at=created_at, created_by=created_by, type_code = type_code)
    #sim_id = 72

    #dt_sim_mac 내용 추가
    # Extract 작업장 values from machine_list dictionaries
    mac_names = [machine["작업장"] for machine in machine_list]
    #print(machine_names)
    insert_sim_macs(sim_id=sim_id, mac_names=mac_names)


    #시뮬레이션 입력 내용 추가
    df_data = pd.DataFrame(job_list)
    insert_sim_input(sim_id=sim_id, df=df_data)

    # 다른 프로세스 실행
    # cmd명령어 실행
    command = python_path + ' sim_run.py  --sim_id "' + str(sim_id) + '" > /dev/null 2>&1'

    print(command)

    # 입력받은 명령어 실행
    if os.name == 'nt':  # Windows
        command = python_path + ' sim_run.py  --sim_id "' + str(sim_id) + '" '
        process = subprocess.Popen(command, shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE)
    else:  # Other operating systems
        process = subprocess.Popen(command, shell=True)

    print(process.pid)

    return { 'status': 'test' , 'sim_id': sim_id}




def serialize_datetime(obj):
    """ datetime 객체를 문자열로 변환 """
    if isinstance(obj, datetime):
        return obj.isoformat()  # 'YYYY-MM-DDTHH:MM:SS.ssssss' 형식
    return obj


def sanitize_ratio(ratio):
    """ ratio 필드를 일관되게 문자열로 변환 """
    return ratio if isinstance(ratio, str) else "16:9"


# port사용 여부 확인(리눅스용)
def is_port_in_use(port):
    """
    특정 포트가 사용 중인지 확인하는 함수.
    리눅스와 윈도우에서 각각 적절한 명령어를 실행.
    """
    system = platform.system()

    if system == "Linux":
        # 리눅스에서 'ss' 또는 'netstat' 명령어로 포트 확인
        try:
            result = subprocess.run(['ss', '-tuln'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if f":{port} " in result.stdout:
                return True
        except FileNotFoundError:
            try:
                result = subprocess.run(['netstat', '-tuln'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if f":{port} " in result.stdout:
                    return True
            except FileNotFoundError:
                print("Neither 'ss' nor 'netstat' command is available.")
                return False
    elif system == "Windows":
        # 윈도우에서 'netstat' 명령어로 포트 확인
        try:
            result = subprocess.run(['netstat', '-an'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if f":{port}" in result.stdout:
                return True
        except FileNotFoundError:
            print("'netstat' command is not available.")
            return False

    else:
        print("Unsupported operating system.")
        return False

    return False


# user_cd에 해당하는 업체코드(comp_id) 반환
def get_comp_id_by_user_cd(user_cd):
    try:
        # PostgreSQL 데이터베이스에 연결
        connection = get_connection()
        cursor = connection.cursor()

        # SQL 쿼리 실행
        query = """
        SELECT comp_id
        FROM public.tb_user_info
        WHERE user_cd = %s;
        """
        cursor.execute(query, (user_cd,))

        # 결과 가져오기
        result = cursor.fetchone()

        if result:
            return result[0]  # comp_id 반환
        else:
            return None  # 해당하는 user_cd가 없는 경우

    except Exception as error:
        print(f"Error: {error}")
        return None

    finally:
        # 연결 종료
        if connection:
            cursor.close()
            connection.close()
        print("Closed to PostgreSQL successfully.")


# ai_server
# ai_server_id를 자동 생성하는 함수
def generate_ai_server_id():
    conn = get_connection()
    cursor = conn.cursor()

    # 현재 테이블에서 가장 큰 ai_server_id 가져오기
    cursor.execute('''
        SELECT ai_server_id 
        FROM public.tb_camera_ai_server 
        WHERE ai_server_id LIKE 'SI%'
        ORDER BY ai_server_id DESC
        LIMIT 1
    ''')

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    if result:
        # 'ser_id_XXXX' 형식에서 숫자 부분 추출 후 1 증가
        last_id = result[0]
        last_number = int(last_id.split('SI')[1])  # 'ser_id_XXXX'에서 XXXX 추출
        new_id = f"SI{last_number + 1:04d}"  # 새로운 'ser_id_XXXX' 생성
    else:
        # 테이블에 데이터가 없으면 첫 번째 ID 생성
        new_id = "SI0001"

    return new_id


# 데이터 삽입 함수
def insert_ai_server(comp_id, server_nm, server_host, api_port, mtx_port, remark, created_by):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # ai_server_id 자동 생성
        ai_server_id = generate_ai_server_id()

        # SQL 쿼리 실행
        cursor.execute('''
            INSERT INTO public.tb_camera_ai_server (comp_id,  ai_server_id, server_nm, server_host, api_port, mtx_port, remark, created_at, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            comp_id, ai_server_id, server_nm, server_host, api_port, mtx_port, remark, datetime.now(), created_by))
        conn.commit()
        print("AI 서버 데이터 삽입 성공.")
        return ai_server_id
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  # 오류 발생 시 트랜잭션 롤백

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return None


# AI 서버 데이터 조회 함수
def get_ai_server_all():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_ai_server')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


# AI 서버 데이터 조회 함수
def get_ai_server_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_ai_server WHERE comp_id = %s', (comp_id,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# AI 서버 데이터 조회 함수
def get_ai_server(ai_server_id):
    ctx = f"[server:{ai_server_id}] get_ai_server"
    conn = get_connection(ctx)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_ai_server WHERE ai_server_id = %s', (ai_server_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print(f"[DB Closed] {ctx}")
    return result


# AI 서버 데이터 업데이트 함수
def update_ai_server(ai_server_id, comp_id=None,
                     server_nm=None, server_host=None,
                     api_port=None, mtx_port=None,
                     remark=None, updated_by=None):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE public.tb_camera_ai_server
            SET comp_id = %s, server_nm = %s, server_host = %s, api_port = %s, mtx_port = %s, remark = %s, updated_at = %s, updated_by = %s
            WHERE ai_server_id = %s
        ''', (
            comp_id, server_nm, server_host, api_port, mtx_port, remark, datetime.now(), updated_by, ai_server_id))
        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount
        conn.commit()

        if affected_rows > 0:
            print(f"Update successful. {affected_rows} row(s) affected.")
            return True
        else:
            print("No rows were updated. Check if ai_server_id exists.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  # 오류 발생 시 트랜잭션 롤백
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# AI 서버 데이터 삭제 함수
def delete_ai_server(ai_server_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute('''DELETE FROM public.tb_camera_ai_server WHERE ai_server_id = %s''', (ai_server_id,))

        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount

        conn.commit()

        if affected_rows > 0:
            print("AI 서버 데이터 삭제 성공.")
            return True
        else:
            print("No rows were deleted. Check if ai_server_id exists.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  # 오류 발생 시 트랜잭션 롤백
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


# camera_id를 자동 생성하는 함수
def generate_camera_id():
    # 현재 테이블에서 가장 큰 camera_id 가져오기
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT camera_id 
        FROM public.tb_camera 
        WHERE camera_id LIKE 'CAM%'
        ORDER BY camera_id DESC
        LIMIT 1
    ''')

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    if result:
        # 'cam_XXXX' 형식에서 숫자 부분 추출 후 1 증가
        last_id = result[0]
        last_number = int(last_id.split('CAM')[1])  # 'cam_XXXX'에서 XXXX 추출
        new_id = f"CAM{last_number + 1:04d}"  # 새로운 'cam_XXXX' 생성
    else:
        # 테이블에 데이터가 없으면 첫 번째 ID 생성
        new_id = "CAM0001"

    return new_id


# CCTV(camera)
# 카메라 삽입 함수 (camera_id 자동 생성)
def insert_camera(comp_id, camera_nm, camera_desc, ai_server_id, rtsp_addr, pid, jit_only, remark, location, created_by):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # camera_id 자동 생성
        camera_id = generate_camera_id()

        cursor.execute('''
            INSERT INTO public.tb_camera (comp_id, camera_id, camera_nm, camera_desc, ai_server_id, rtsp_addr, out_path, pid, jit_only, remark, location, created_at, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ''', (comp_id, camera_id, camera_nm, camera_desc, ai_server_id, rtsp_addr, camera_id, pid, jit_only, remark, location,
              created_by))
        conn.commit()
        print(f"Camera with ID {camera_id} inserted successfully.")
        return camera_id
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return None


# 모든 카메라 내용 조회
def get_all_cameras():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera')
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 특정 카메라 내용 조회
def get_camera_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera WHERE comp_id = %s', (comp_id,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 특정 카메라 내용 조회
def get_camera_by_id(camera_id):
    ctx = f"[{camera_id}] get_camera_by_id"
    conn = get_connection(ctx)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera WHERE camera_id = %s', (camera_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print(f"[DB Closed] {ctx}")
    return result


# 특정 카메라 내용 조회
def get_camera_server_by_id(camera_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT c.camera_id, s.server_host, api_port FROM public.tb_camera c
                      LEFT JOIN public.tb_camera_ai_server s
                        on c.ai_server_id = s.ai_server_id
                        WHERE camera_id = %s''', (camera_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 데이터 수정(pid)
def update_camera_pid(camera_id, pid, port, run_yn=False):
    ctx = f"[{camera_id}] update_camera_pid"
    conn = get_connection(ctx)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE public.tb_camera
            SET pid = %s, port_number = %s, run_yn = %s
            WHERE camera_id = %s
        ''', (pid, port, run_yn, camera_id))
        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount
        conn.commit()
        if affected_rows > 0:
            print(f"[{camera_id}] Camera updated successfully")
            return True
        else:
            print(f"[{camera_id}] No rows were updated. Check if camera_id exists.")
            return False

    except Exception as e:
        print(f"[{camera_id}] Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print(f"[DB Closed] {ctx}")
    return False


# 데이터 수정
def update_camera(camera_id, comp_id=None, camera_nm=None, camera_desc=None, ai_server_id=None, rtsp_addr=None,
                  pid=None, jit_only=None, remark=None, location=None, updated_by=None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE public.tb_camera
            SET comp_id = %s, camera_nm = %s, camera_desc = %s, ai_server_id = %s, rtsp_addr = %s, pid = %s, jit_only = %s, remark = %s, location = %s, updated_at = NOW(), updated_by = %s
            WHERE camera_id = %s
        ''', (
            comp_id, camera_nm, camera_desc, ai_server_id, rtsp_addr, pid, False if jit_only == "" else jit_only,
            remark, location,
            updated_by, camera_id))
        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount
        conn.commit()
        if affected_rows > 0:
            print("Camera updated successfully")
            return True
        else:
            print("No rows were updated. Check if camera_id exists.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


# 데이터 삭제
def delete_camera(camera_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM public.tb_camera WHERE camera_id = %s', (camera_id,))
        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount
        conn.commit()

        if affected_rows > 0:
            print("Camera deleted successfully")
            return True
        else:
            print("No rows were deleted. Check if camera_id exists.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return False


# 모니터링 프로파일(tb_camera_monitoring_grp)
# monitoring_grp_id 자동 생성하는 함수
def generate_monitoring_grp_id():
    # 현재 테이블에서 가장 큰 camera_id 가져오기
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT monitoring_grp_id 
        FROM public.tb_camera_monitoring_grp 
        WHERE monitoring_grp_id LIKE 'CMG%'
        ORDER BY monitoring_grp_id DESC
        LIMIT 1
    ''')

    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    if result:
        # 'CMGXXXX' 형식에서 숫자 부분 추출 후 1 증가
        last_id = result[0]
        last_number = int(last_id.split('CMG')[1])  # 'cam_XXXX'에서 XXXX 추출
        new_id = f"CMG{last_number + 1:04d}"  # 새로운 'cam_XXXX' 생성
    else:
        # 테이블에 데이터가 없으면 첫 번째 ID 생성
        new_id = "CMG0001"

    return new_id


# 추가
def insert_camera_monitoring_grp(comp_id, grp_nm, created_by):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # camera_id 자동 생성
        monitoring_grp_id = generate_monitoring_grp_id()

        cursor.execute('''
            INSERT INTO public.tb_camera_monitoring_grp (comp_id, monitoring_grp_id, grp_nm, created_at, created_by)
            VALUES (%s, %s, %s, NOW(), %s)
        ''', (comp_id, monitoring_grp_id, grp_nm, created_by))

        conn.commit()
        print(f"tb_camera_monitoring_grp with ID {monitoring_grp_id} inserted successfully.")
        return monitoring_grp_id
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return None


# 전체 조회
def get_all_camera_monitoring_grps():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_monitoring_grp')
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 특정 내용 조회
def get_camera_monitoring_grp_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_monitoring_grp WHERE comp_id = %s', (comp_id,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 특정 내용 조회
def get_camera_monitoring_grp_by_id(monitoring_grp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_monitoring_grp WHERE monitoring_grp_id = %s', (monitoring_grp_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 데이터 수정(pid)
def update_camera_monitoring_grp(monitoring_grp_id, grp_nm=None, updated_by=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_monitoring_grp
            SET grp_nm = %s, updated_at = NOW(), updated_by = %s
            WHERE monitoring_grp_id = %s
        ''', (grp_nm, updated_by, monitoring_grp_id))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera monitoring group updated successfully.")
            return True
        else:
            print("No rows were updated.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return False


# 삭제
def delete_camera_monitoring_grp(monitoring_grp_id):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('DELETE FROM public.tb_camera_monitoring_grp WHERE monitoring_grp_id = %s', (monitoring_grp_id,))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera monitoring group deleted successfully.")
            return True

        else:
            print("No rows were deleted.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


# 모니터링 프로파일 상세(tb_camera_monitoring_layout)

# monitoring_grp_id에 해당하는 레코드 수를 검색하고, 그 수에 1을 더해서 반환하는 함수
def get_next_item_idx(monitoring_grp_id):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        # monitoring_grp_id에 해당하는 레코드 수를 검색
        cursor.execute('''
            SELECT COUNT(*)
            FROM public.tb_camera_monitoring_layout
            WHERE monitoring_grp_id = %s
        ''', (monitoring_grp_id,))

        count = cursor.fetchone()[0]  # 레코드 수 가져오기

        next_item_idx = count + 1  # 레코드 수에 1을 더한 값
        return next_item_idx

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 추가
def insert_camera_monitoring_layout(monitoring_grp_id, coordinate_x, coordinate_y, item_width, item_height, camera_id,
                                    created_by, title):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        item_idx = get_next_item_idx(monitoring_grp_id)
        cursor.execute('''
            INSERT INTO public.tb_camera_monitoring_layout (
                monitoring_grp_id, item_idx, coordinate_x, coordinate_y, item_width, item_height, camera_id, created_at, created_by, title
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ''', (monitoring_grp_id, item_idx, coordinate_x, coordinate_y, item_width, item_height, camera_id, created_by,
              title,))

        conn.commit()
        print("Camera monitoring layout inserted successfully.")

        return monitoring_grp_id, item_idx
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 전체 내용 조회
def get_all_camera_monitoring_layouts():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT * FROM public.tb_camera_monitoring_layout')
        rows = cursor.fetchall()

        return rows

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 특정 그룹과 아이템 인텍스로 데이터 조회
def get_camera_monitoring_layout_by_id(monitoring_grp_id, item_idx):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            SELECT * FROM public.tb_camera_monitoring_layout 
            WHERE monitoring_grp_id = %s AND item_idx = %s
        ''', (monitoring_grp_id, item_idx))

        row = cursor.fetchone()

        if row:
            print(row)
            return row
        else:
            print("Camera monitoring layout not found.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 수정
def update_camera_monitoring_layout(monitoring_grp_id, item_idx, coordinate_x=None, coordinate_y=None, item_width=None,
                                    item_height=None, camera_id=None, updated_by=None, title=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_monitoring_layout
            SET coordinate_x = %s, coordinate_y = %s, item_width = %s, item_height = %s, camera_id = %s, updated_at = NOW(), updated_by = %s,
                       title = %s
            WHERE monitoring_grp_id = %s AND item_idx = %s
        ''', (
            coordinate_x, coordinate_y, item_width, item_height, camera_id, updated_by, title, monitoring_grp_id,
            item_idx))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera monitoring layout updated successfully.")
            return monitoring_grp_id, item_idx
        else:
            print("No rows were updated.")
            return None
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 삭제
def delete_camera_monitoring_layout(monitoring_grp_id, item_idx):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('DELETE FROM public.tb_camera_monitoring_layout WHERE monitoring_grp_id = %s AND item_idx = %s',
                       (monitoring_grp_id, item_idx))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera monitoring layout deleted successfully.")
            return True
        else:
            print("No rows were deleted.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# monitoring_grp_id에 해당하는 값만 조회
# 특정 그룹과 아이템 인텍스로 데이터 조회
def get_camera_monitoring_layout_by_monitoring_grp_id(monitoring_grp_id):
    conn = get_connection()
    cursor = conn.cursor()

    # try:
    cursor.execute('''
        SELECT * FROM public.tb_camera_monitoring_layout 
        WHERE monitoring_grp_id = %s
    ''', (monitoring_grp_id,))

    result = cursor.fetchall()

    print(result)
    # return result

    # except Exception as e:
    #     print(f"Error: {e}")
    #     return None

    # finally:
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    return result


# ai 모델(tb_camera_ai_model)
# 추가
def insert_tb_camera_ai_model(model_nm, model_txt, created_by):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO public.tb_camera_ai_model (model_nm, model_txt, created_at, created_by)
            VALUES (%s, %s, NOW(), %s)
        ''', (model_nm, model_txt, created_by))

        conn.commit()
        print("AI Model inserted successfully.")

        return model_nm
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 전체 조회
def get_all_tb_camera_ai_models():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT * FROM public.tb_camera_ai_model')
        rows = cursor.fetchall()

        return rows

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 특정 모델 조회
def get_tb_camera_ai_model_by_name(model_nm):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT * FROM public.tb_camera_ai_model WHERE model_nm = %s', (model_nm,))
        row = cursor.fetchone()

        if row:
            print(row)
            return row
        else:
            print("AI Model not found.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 수정
def update_tb_camera_ai_model(model_nm, model_txt=None, updated_by=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_ai_model
            SET model_txt = %s, updated_at = NOW(), updated_by = %s
            WHERE model_nm = %s
        ''', (model_txt, updated_by, model_nm))

        conn.commit()

        if cursor.rowcount > 0:
            print("AI Model updated successfully.")
            return model_nm
        else:
            print("No rows were updated.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 삭제
def delete_tb_camera_ai_model(model_nm):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('DELETE FROM public.tb_camera_ai_model WHERE model_nm = %s', (model_nm,))

        conn.commit()

        if cursor.rowcount > 0:
            print("AI Model deleted successfully.")
            return True
        else:
            print("No rows were deleted.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# roi(tb_camera_roi)
# 추가
def insert_camera_roi(camera_id, point, model_nm, is_run, created_by):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO public.tb_camera_roi (camera_id, point, model_nm, created_at, created_by, is_run)
            VALUES (%s, %s, %s, NOW(), %s,  %s)
        ''', (camera_id, point, model_nm, created_by, is_run))

        conn.commit()
        print("Camera ROI inserted successfully.")
        return camera_id, model_nm

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return None


# 전체 조회
def get_all_camera_rois():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_roi')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


def get_camera_roi_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            SELECT
                tc.camera_id,
                tc.camera_nm,
                tc.camera_desc,
                cr.point,
                cr.model_nm,
                cr.roi_id,
                cr.is_run,
                cr.created_at,
                cr.created_by,
                cr.updated_at,
                cr.updated_by
            FROM public.tb_camera tc
            LEFT JOIN public.tb_camera_roi cr
                ON cr.camera_id = tc.camera_id
            WHERE tc.comp_id = %s
            ORDER BY tc.camera_id, cr.model_nm, cr.roi_id''', (comp_id,))
        row = cursor.fetchall()

        if row:
            print(row)
            return row
        else:
            print("Camera ROI not found.")
            return None

    except Exception as e:
        print(f"Error: {e}")
        return []

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    # return None


def get_camera_roi_by_id(camera_id, model_nm):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('SELECT * FROM public.tb_camera_roi WHERE camera_id = %s and model_nm=%s',
                       (camera_id, model_nm,))
        row = cursor.fetchone()

        if row:
            print(row)
            return row
        else:
            print("Camera ROI not found.")
            return None

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return None


def update_camera_roi(camera_id, model_nm, point=None, updated_by=None, is_run=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_roi
            SET point = %s, updated_at = NOW(), updated_by = %s, is_run = %s
            WHERE camera_id = %s and model_nm=%s
        ''', (point, updated_by, is_run, camera_id, model_nm))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera ROI updated successfully.")
            return True
        else:
            print("No rows were updated.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

def upsert_camera_roi(camera_id, model_nm, roi_id, point_str, user_cd, is_run):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO public.tb_camera_roi
                (camera_id, model_nm, roi_id, point, created_at, created_by, updated_at, updated_by, is_run)
            VALUES
                (%s, %s, %s, %s, NOW(), %s, NOW(), %s, %s)
            ON CONFLICT (camera_id, model_nm, roi_id)
            DO UPDATE SET
                point = EXCLUDED.point,
                updated_at = NOW(),
                updated_by = EXCLUDED.updated_by,
                is_run = EXCLUDED.is_run
        """, (camera_id, model_nm, roi_id, point_str, user_cd, user_cd, is_run))
        conn.commit()
        return True
    except Exception as e:
        print(f"Error: {e}", flush=True)
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def delete_camera_roi(camera_id, model_nm):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('DELETE FROM public.tb_camera_roi WHERE camera_id = %s and model_nm=%s', (camera_id, model_nm,))

        conn.commit()

        if cursor.rowcount > 0:
            print("Camera ROI deleted successfully.")
            return True
        else:
            print("No rows were deleted.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# tb_camera_event_hist  crud 코드
# 추가
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

        return event_time, camera_id
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return None

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 수정
def update_camera_event_hist(event_time, camera_id, isRead=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_event_hist
            SET isRead = %s
            WHERE event_time = %s AND camera_id = %s
        ''', (isRead, event_time, camera_id))

        conn.commit()

        if cursor.rowcount > 0:
            print("camera_event updated successfully.")
            return True
        else:
            print("No rows were updated.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# 비고 입력
def update_camera_event_hist_remart(event_time, camera_id, remark=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            UPDATE public.tb_camera_event_hist
            SET remark = %s
            WHERE event_time = %s AND camera_id = %s
        ''', (remark, event_time, camera_id))

        conn.commit()

        if cursor.rowcount > 0:
            print("camera_event updated successfully.")
            return True
        else:
            print("No rows were updated.")
            return False
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


def get_all_camera_event_hist():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT * FROM public.tb_camera_event_hist where event_time::DATE = CURRENT_DATE  ORDER BY event_time DESC')
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_camera_event_count(location=None):
    conn = get_connection()
    cursor = conn.cursor()
    #테스트용으로 날짜 고정 수정 필요.
    if location == None:
        cursor.execute('''
            SELECT
                tceh.event_type,
                tceh.event_desc,
                COUNT(*) AS event_count,
                BOOL_OR(NOT tceh.isread) AS alarm_check  -- 하나라도 false 있으면 true
            FROM public.tb_camera_event_hist tceh
            LEFT JOIN public.tb_camera tc 
                ON tc.camera_id = tceh.camera_id
            -- WHERE tceh.event_time::DATE = '2025-08-18'
            WHERE tceh.event_time::DATE = CURRENT_DATE
            GROUP BY tceh.event_type, tceh.event_desc
            ORDER BY event_count DESC;
            ''')
    else:
        cursor.execute('''
            SELECT
                tceh.event_type,
                tceh.event_desc,
                COUNT(*) AS event_count,
                BOOL_OR(NOT tceh.isread) AS alarm_check  -- 하나라도 false 있으면 true
            FROM public.tb_camera_event_hist tceh
            LEFT JOIN public.tb_camera tc 
                ON tc.camera_id = tceh.camera_id
            -- WHERE tceh.event_time::DATE = '2025-08-18'
            WHERE tceh.event_time::DATE = CURRENT_DATE
                and tc."location" = %s
            GROUP BY tceh.event_type, tceh.event_desc
            ORDER BY event_count DESC;
            ''', (location,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_all_camera_event_hist1_by_location(location):
    conn = get_connection()
    cursor = conn.cursor()
    #테스트용으로 날짜 고정 수정 필요.
    cursor.execute('''
        select
            tceh.event_time,
            tceh.camera_id,
            tc."location",
            tceh.event_type,
            tceh.event_desc,
            tceh.file_path,
            tceh.isread,
            tceh.remark
        FROM public.tb_camera_event_hist tceh
        left join public.tb_camera tc on tc.camera_id = tceh.camera_id
        -- where event_time::DATE = \'2025-08-18\'
        where event_time::DATE = CURRENT_DATE
              and tc.location = %s
        ORDER BY event_time DESC
        ''', (location, ))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def fn_get_unread_event_counts(camera_id: str, event_date: str | None = None):
    """
    tb_camera_event_hist 기준으로
    특정 카메라의 미확인 이벤트를 event_type, event_desc별로 GROUP BY 해서 반환.

    :param camera_id: 카메라 ID (필수)
    :param event_date: 조회 날짜 (YYYY-MM-DD), None이면 CURRENT_DATE 사용
    :return: (rows, error) 형식, rows는 dict 리스트
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        if event_date:
            # 지정 날짜 기준
            sql = """
                SELECT
                    tceh.event_type,
                    tceh.event_desc,
                    COUNT(*) AS unread_count
                FROM public.tb_camera_event_hist tceh
                LEFT JOIN public.tb_camera tc ON tc.camera_id = tceh.camera_id
                WHERE event_time::DATE = %s::DATE
                  AND tceh.isread = FALSE
                  AND tc.camera_id = %s
                GROUP BY tceh.event_type, tceh.event_desc
                ORDER BY tceh.event_type, tceh.event_desc;
            """
            cur.execute(sql, (event_date, camera_id))
        else:
            # 오늘(CURRENT_DATE) 기준
            sql = """
                SELECT
                    tceh.event_type,
                    tceh.event_desc,
                    COUNT(*) AS unread_count
                FROM public.tb_camera_event_hist tceh
                LEFT JOIN public.tb_camera tc ON tc.camera_id = tceh.camera_id
                WHERE event_time::DATE = CURRENT_DATE
                  AND tceh.isread = FALSE
                  AND tc.camera_id = %s
                GROUP BY tceh.event_type, tceh.event_desc
                ORDER BY tceh.event_type, tceh.event_desc;
            """
            cur.execute(sql, (camera_id,))

        rows = cur.fetchall()
        return rows, None

    except Exception as e:
        return None, str(e)

    finally:
        if conn:
            conn.close()




def get_all_camera_event_hist1_by_event_type(event_type, location=None):
    conn = get_connection()
    cursor = conn.cursor()
    #테스트용으로 날짜 고정 수정 필요.
    if location != None:
        cursor.execute('''
            select
                tceh.event_time,
                tceh.camera_id,
                tc."location",
                tceh.event_type,
                tceh.event_desc,
                tceh.file_path,
                tceh.isread,
                tceh.remark
            FROM public.tb_camera_event_hist tceh
            left join public.tb_camera tc on tc.camera_id = tceh.camera_id
            -- where event_time::DATE = \'2025-08-18\'
            where event_time::DATE = CURRENT_DATE
                and tceh.event_type = %s       
                and tc.location = %s
            ORDER BY event_time DESC
            ''', (event_type, location))
    else:
        cursor.execute('''
            select
                tceh.event_time,
                tceh.camera_id,
                tc."location",
                tceh.event_type,
                tceh.event_desc,
                tceh.file_path,
                tceh.isread,
                tceh.remark
            FROM public.tb_camera_event_hist tceh
            left join public.tb_camera tc on tc.camera_id = tceh.camera_id
            -- where event_time::DATE = \'2025-08-18\'
            where event_time::DATE = CURRENT_DATE
                and tceh.event_type = %s 
            ORDER BY event_time DESC
            ''', (event_type, ))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def get_all_camera_event_hist_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
            SELECT ceh.*
            FROM public.tb_camera_event_hist ceh
            left join public.tb_camera tc on ceh.camera_id = tc.camera_id
            where tc.comp_id = %s
            ORDER BY event_time DESC
        ''', (comp_id,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


def get_all_camera_event_hist_by_group(grp_id):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            date_trunc('second', ceh.event_time) AS event_time,
            ceh.event_type,
            ceh.event_desc,
            json_agg(
                json_build_object(
                    'camera_id', tc.camera_id,
                    'file_path', ceh.file_path
                )
                ORDER BY tc.camera_id, ceh.file_path
            ) AS images,
            bool_and(ceh.isread) AS isread,
            max(ceh.remark) AS remark
        FROM public.tb_camera_event_hist ceh
        LEFT JOIN public.tb_camera tc ON ceh.camera_id = tc.camera_id
        LEFT JOIN public.tb_camera_monitoring_layout tcml ON tcml.camera_id = tc.camera_id
        LEFT JOIN public.tb_camera_monitoring_grp tcmg ON tcmg.monitoring_grp_id = tcml.monitoring_grp_id
        WHERE tcmg.monitoring_grp_id = %s
          AND ceh.event_time::date = CURRENT_DATE
        GROUP BY
            date_trunc('second', ceh.event_time),
            ceh.event_type,
            ceh.event_desc
        ORDER BY event_time DESC
    """

    cursor.execute(query, (grp_id,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def get_camera_event_alert_by_group(grp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            ceh.camera_id,     
            COUNT(*) AS total_cnt,
            SUM(CASE WHEN ceh.isread = false THEN 1 ELSE 0 END) AS unread_cnt,
            MAX(ceh.event_time) AS last_event_time
        FROM public.tb_camera_event_hist ceh
        LEFT JOIN public.tb_camera tc
            ON ceh.camera_id = tc.camera_id
        LEFT JOIN public.tb_camera_monitoring_layout tcml
            ON tcml.camera_id = tc.camera_id
        LEFT JOIN public.tb_camera_monitoring_grp tcmg
            ON tcmg.monitoring_grp_id = tcml.monitoring_grp_id
        WHERE tcmg.monitoring_grp_id = %s
            AND ceh.event_time::DATE = CURRENT_DATE
        GROUP BY ceh.camera_id
        ORDER BY last_event_time DESC
    ''', (grp_id,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def get_all_camera_event_hist_group_list(comp_id):
    conn = get_connection()
    cursor = conn.cursor()

    query = ''' 
                SELECT tcmg.monitoring_grp_id, 
                        COUNT(*) FILTER (WHERE ceh.isread = FALSE) AS isread_count
                FROM public.tb_camera_event_hist ceh
                left join public.tb_camera tc on ceh.camera_id = tc.camera_id
                left join public.tb_camera_monitoring_layout tcml on tcml.camera_id = tc.camera_id 
                left join public.tb_camera_monitoring_grp tcmg on tcmg.monitoring_grp_id = tcml.monitoring_grp_id 
                where ceh.event_time::DATE = CURRENT_DATE 
            '''

    # grp_id None이 아닌 경우 WHERE 절에 추가
    if comp_id is not None:
        query += " AND tcmg.comp_id = '" + comp_id + "' "

    # monitoring_grp_id  구룹으로 묶음
    query += ' group by tcmg.monitoring_grp_id '

    last_query = "select tcmg.monitoring_grp_id, sub.isread_count \
                    from public.tb_camera_monitoring_grp tcmg \
                    left join (" + query + ") sub on tcmg.monitoring_grp_id = sub.monitoring_grp_id \
                    where 1=1"

    # grp_id None이 아닌 경우 WHERE 절에 추가
    if comp_id is not None:
        last_query += " AND tcmg.comp_id = '" + comp_id + "' "

    last_query += ' ORDER BY tcmg.monitoring_grp_id asc'

    # 쿼리 실행
    print(last_query)
    cursor.execute(last_query)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


def get_camera_event_serch(start_date, end_date, comp_id, cctv_id, event_type):
    conn = get_connection()
    cursor = conn.cursor()

    # 기본 SELECT 쿼리
    query = '''
                    SELECT ceh.*
                    FROM public.tb_camera_event_hist ceh
                    left join public.tb_camera tc on ceh.camera_id = tc.camera_id
                    where ceh.event_time between %s and %s
            '''

    # WHERE 조건 추가를 위한 값과 인자 리스트
    params = []

    params.append(start_date + ' 00:00:00')
    params.append(end_date + ' 23:59:59')

    # grp_id None이 아닌 경우 WHERE 절에 추가
    if comp_id is not None:
        query += ' AND tc.comp_id = %s '
        params.append(comp_id)

    # cctv_id None이 아닌 경우 WHERE 절에 추가
    if cctv_id is not None:
        query += ' AND tc.camera_id = %s '
        params.append(cctv_id)

    # event_type None이 아닌 경우 WHERE 절에 추가
    if event_type is not None:
        query += ' AND ceh.event_type = %s '
        params.append(event_type)

    # event_time으로 내림차순 정렬
    query += ' ORDER BY event_time DESC'

    # 쿼리 실행
    print(query)
    print(params)
    cursor.execute(query, tuple(params))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


# 배열
def get_camera_event_serch1(start_date, end_date, comp_id, cctv_id, event_type):
    conn = get_connection()
    cursor = conn.cursor()

    # 기본 SELECT 쿼리
    query = '''
                    SELECT ceh.*
                    FROM public.tb_camera_event_hist ceh
                    left join public.tb_camera tc on ceh.camera_id = tc.camera_id
                    where ceh.event_time between %s and %s
            '''

    # WHERE 조건 추가를 위한 값과 인자 리스트
    params = []

    params.append(start_date + ' 00:00:00')
    params.append(end_date + ' 23:59:59')

    # grp_id None이 아닌 경우 WHERE 절에 추가
    if comp_id is not None:
        query += ' AND tc.comp_id = %s '
        params.append(comp_id)

    # cctv_id None이 아닌 경우 WHERE 절에 추가
    if len(cctv_id) != 0:
        # Convert camera_ids list into a string that can be used in SQL IN clause
        camera_ids_sql = ', '.join([f"'{camera_id}'" for camera_id in cctv_id])
        query += f' AND tc.camera_id IN ({camera_ids_sql}) '

    # event_type None이 아닌 경우 WHERE 절에 추가
    if len(event_type) != 0:
        # Convert camera_ids list into a string that can be used in SQL IN clause
        event_type_ids_sql = ', '.join([f"'{type}'" for type in event_type])
        query += f' AND ceh.event_type IN ({event_type_ids_sql}) '

    # event_time으로 내림차순 정렬
    query += ' ORDER BY event_time DESC'

    # 쿼리 실행
    print(query)
    print(params)
    cursor.execute(query, tuple(params))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return rows


# 파일에서 데이터 읽기
def load_data(data_file):
    try:
        with open(data_file, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


# 파일에 데이터 쓰기
def save_data(data, data_file):
    with open(data_file, 'w') as file:
        json.dump(data, file, indent=4)


# 데이터 저장할 파일 경로
CCTV_DATA_FILE = './data/cctv_data.json'
# 데이터 저장할 파일 경로
USER_DATA_FILE = './data/user_data.json'
# 모니터링 프로필 마스터
PROFILE_DATA_FILE = './data/profile_server_data.json'
PRO_DETAIL_DATA_FILE = './data/pro_detail_server_data.json'


# url 받기
def get_play_url(cctv_id):
    # cctv_id에 해당되는 CCTV_DATA 로드
    cctv_data = get_camera_by_id(cctv_id)
    print('cctv_data')
    print(cctv_data)
    print('cctv_data[6]')
    print(cctv_data)
    server_id = cctv_data[4]
    # server_ip = get_ai_server(server_id)[3]   

    server_url = get_ai_server(server_id)[11]

    if server_url == None:
        server_url = 'http://' + get_ai_server(server_id)[3] + ':' + get_ai_server(server_id)[5]

    # POST 요청을 보낼 URL
    # url = server_url + '/stream/' + cctv_data[6] + '/'
    url = f"{server_url}/{cctv_data[6]}/?controls=0"
    print('url')
    print(url)

    return url


# 비율 받기
def get_ratio(cctv_id):

    # cctv_id에 해당되는 CCTV_DATA 로드
    cctv_data = get_camera_by_id(cctv_id)

    server_id = cctv_data[4]
    server_data = get_ai_server(server_id)
    server_ip = server_data[3]

    restapi_port = server_data[4]
    out_path = cctv_data[6]

    # POST 요청을 보낼 URL
    url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/get_img/' + out_path
    print(url)

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    # POST 요청 보내기
    response = requests.get(url, headers=headers)
    # 응답 처리
    if response.status_code == 200:
        print("POST 요청 성공")
        # print("응답 데이터:", response.json())
        # return jsonify({"message": f"stop successfully"})
        # print(response.json())

        # height와 width 값을 입력받음
        dimensions = response.json()['img_size']

        # height와 width의 최대공약수(GCD) 구하기
        gcd = math.gcd(dimensions['height'], dimensions['width'])

        # 비율 계산
        width_ratio = dimensions['width'] // gcd
        height_ratio = dimensions['height'] // gcd

        return f"{width_ratio}:{height_ratio}"
    else:
        print("POST 요청 실패")
        print("상태 코드:", response.status_code)
        print("응답 내용:", response.text)

    return {"message: err"}


MODEL_DATA_FILE = './data/model_data.json'


# 서버 종료시 실행
def on_exit():
    print("Flask 애플리케이션이 종료되었습니다.")
    # 종료 시 필요한 작업 수행
    for key in process_dict.keys():
        try:
            # 프로세스 트리를 강제 종료
            print(key)
            terminate_process_tree(key)
        except Exception as e:
            print(str(key) + '인 프로세스가 종료되지 않았습니다.')


# 프로그램 종료 시 on_exit 함수가 호출되도록 등록
atexit.register(on_exit)


# 이미지 사이즈 받기
def get_imgfile_size(out_path):
    print(out_path)
    img_nm = find_largest_jpg_file('./img/' + out_path + '_short')
    # print(img_nm)
    width, height = get_image_size('./img/' + out_path + '_short/' + img_nm)
    return width, height


# 초기 list 실행코드
def init_run_cctv(in_url, out_path):
    # cmd명령어 실행
    command = python_path + 'rtsp_service/test_deepsort.py  --in_url "' + in_url + '" --out_path "' + out_path + '"  > /dev/null 2>&1'

    # 입력받은 명령어 실행
    if os.name == 'nt':  # Windows
        command = python_path + 'rtsp_service/test_deepsort.py  --in_url "' + in_url + '" --out_path "' + out_path + '"'
        process = subprocess.Popen(command, shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE)
    else:  # Other operating systems
        process = subprocess.Popen(command, shell=True)

    process_dict[process.pid] = process


# def terminate_process_tree(pid):
#     """특정 프로세스 및 자식 프로세스를 모두 강제 종료"""
#     try:
#         parent = psutil.Process(pid)
#         children = parent.children(recursive=True)
#         for child in children:
#             child.kill()
#         parent.kill()
#     except psutil.NoSuchProcess:
#         pass
def terminate_process_tree(pid: int) -> bool:
    """pid 및 자식 프로세스 종료. 종료 시도했으면 True, 이미 없었으면 False."""
    try:
        parent = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return False

    # 자식부터
    children = parent.children(recursive=True)

    # 1) graceful terminate
    for p in children:
        try: p.terminate()
        except psutil.NoSuchProcess: pass
    try:
        parent.terminate()
    except psutil.NoSuchProcess:
        return False

    # 2) wait a bit
    gone, alive = psutil.wait_procs(children + [parent], timeout=3)

    # 3) still alive -> kill
    for p in alive:
        try: p.kill()
        except psutil.NoSuchProcess: pass

    return True


# 비밀번호 암호화 함수 (문자열로 변환)
def encrypt_password(password: str) -> str:
    # 비밀번호를 바이트로 변환
    password_bytes = password.encode('utf-8')
    # 솔트 생성 및 해시
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password_bytes, salt)
    # 해시된 비밀번호를 문자열로 변환
    return hashed_password.decode('utf-8')


# 비밀번호 검증 함수
def verify_password(password: str, hashed_password: str) -> bool:
    # 비밀번호를 바이트로 변환
    password_bytes = password.encode('utf-8')
    # 해시된 비밀번호를 다시 바이트로 변환
    hashed_password_bytes = hashed_password.encode('utf-8')
    # 입력된 비밀번호와 해시된 비밀번호를 비교
    return bcrypt.checkpw(password_bytes, hashed_password_bytes)


# 가징 최근 이미지 파일명 받기
def find_largest_jpg_file(directory):
    largest_file = None
    largest_number = -1

    try:
        files = os.listdir(directory)
        for f in files:
            # 파일이 .jpg 확장자이며, 파일명이 숫자인지 확인
            if f.endswith('.jpg') and f[:-4].isdigit():
                file_number = int(f[:-4])  # 파일명에서 숫자 부분만 추출
                # 가장 큰 숫자 파일명인지 확인
                if file_number > largest_number:
                    largest_number = file_number
                    largest_file = f

    except Exception as e:
        print(f"Error: {e}")

    return largest_file


# 이미지 사이즈 받기
def get_image_size(image_path):
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            return width, height
    except Exception as e:
        print(f"Error: {e}")
        current_directory = os.getcwd()
        new_path = current_directory + image_path.replace('..', '')
        print(new_path)
        with Image.open(new_path) as img:
            width, height = img.size
            return width, height
        return None


###원격 구동 api
def get_camera_by_id1(camera_id):
    """
    Retrieves camera information from the database based on camera_id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT ai_server_id, out_path, pid, rtsp_addr FROM public.tb_camera WHERE camera_id = %s',
                   (camera_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    if result:
        return {
            "ai_server_id": result[0],
            "out_path": result[1],
            "pid": result[2],
            "rtsp_addr": result[3]
        }
    return None


def get_ai_server_info(ai_server_id):
    """
    Retrieves AI server information from the database based on ai_server_id.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT server_host, api_port FROM public.tb_camera_ai_server WHERE ai_server_id = %s',
                   (ai_server_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    if result:
        return {
            "server_host": result[0],
            "api_port": result[1]
        }
    return None


###안전 관리자 메뉴###
def generate_manager_id():
    """
    기존 tb_telegram_managers에서 가장 최근의 manager_id를 기반으로 새로운 manager_id 생성.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT manager_id FROM public.tb_telegram_managers 
        ORDER BY registered_at DESC LIMIT 1
    ''')
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    if result:
        last_id = result[0]  # 가장 최근의 manager_id
        last_number = int(last_id.split('_')[-1])  # 숫자 부분 추출
        new_id = f"MGR_{last_number + 1:04d}"  # 새로운 manager_id 생성 (예: MGR_0002)
    else:
        new_id = "MGR_0001"  # 데이터가 없으면 첫 번째 ID 생성

    return new_id


# 카메라 모니터링 그룹 조회 함수
def get_group_info_by_comp_id(comp_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_camera_monitoring_grp WHERE comp_id = %s', (comp_id,))
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 구역 관리 그룹 정보 조회
def get_monitoring_grp():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT monitoring_grp_id, grp_nm FROM public.tb_camera_monitoring_grp')
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 안전 관리자 데이터 전체 조회
def get_all_managers():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_telegram_managers')
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


def get_mt_manager(manager_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM public.tb_telegram_managers WHERE manager_id = %s', (manager_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")
    return result


# 안전 관리자 삽입 함수 (monitoring_grp_id 자동 생성)
def insert_safety_manager(monitoring_grp_id=None, chat_id=None, notification_on=None, created_by=None, comp_id=None, token=None):
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if notification_on in ["", None]:
            notification_on = False
        elif isinstance(notification_on, str):
            notification_on = notification_on.lower() in ["true", "1", "t", "yes"]

        # ✅ INSERT 후 monitoring_grp_id와 chat_id 반환
        cursor.execute('''
            INSERT INTO public.tb_telegram_managers (monitoring_grp_id, chat_id, notification_on, created_at, created_by, comp_id, token)
            VALUES (%s, %s, %s, NOW(), %s,%s, %s)
            RETURNING monitoring_grp_id, chat_id
        ''', (monitoring_grp_id, chat_id, notification_on, created_by, comp_id, token))

        result = cursor.fetchone()  # ✅ 삽입된 키 값 가져오기
        conn.commit()

        return result  # ✅ (monitoring_grp_id, chat_id) 튜플 반환

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return None


# 데이터 수정
def update_safety_manager(monitoring_grp_id, chat_id, token,
                          notification_on, updated_by, original_monitoring_grp_id, original_chat_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # ✅ 기존 데이터를 기준으로 업데이트
        cursor.execute('''
            UPDATE public.tb_telegram_managers
            SET monitoring_grp_id = %s, 
                chat_id = %s,
                token = %s,
                notification_on = %s, 
                updated_at = NOW(), 
                updated_by = %s
            WHERE monitoring_grp_id = %s AND chat_id = %s;
        ''', (monitoring_grp_id, chat_id, token,
              False if notification_on == "" else notification_on,
              updated_by, original_monitoring_grp_id, original_chat_id))

        affected_rows = cursor.rowcount
        conn.commit()

        return affected_rows > 0  # ✅ 업데이트 성공 여부 반환

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


def update_monitoring_grps(monitoring_grp_id, grp_nm):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE public.tb_camera_monitoring_grp
            SET grp_nm = %s
            WHERE monitoring_grp_id = %s
        ''', (
            grp_nm,
            monitoring_grp_id))
        # 업데이트된 행의 수를 확인
        affected_rows = cursor.rowcount
        conn.commit()
        if affected_rows > 0:
            print("monitoring_grp_id updated successfully")
            return True
        else:
            print("No rows were updated. Check if monitoring_grp_id exists.")
            return False

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


def return_monitoring_grp_id(comp_id, grp_nm, created_by):
    monitoring_grp_id = generate_monitoring_grp_id()  # ID 생성
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO public.tb_camera_monitoring_grp (comp_id, monitoring_grp_id, grp_nm, created_at, created_by)
        VALUES (%s, %s, %s, NOW(), %s)
        RETURNING monitoring_grp_id
    ''', (comp_id, monitoring_grp_id, grp_nm, created_by))

    result = cursor.fetchone()
    conn.commit()
    cursor.close()
    conn.close()
    print("Closed to PostgreSQL successfully.")

    if result:
        return result[0]  # 생성된 monitoring_grp_id 반환
    return None


# 데이터 삭제
def delete_manager(monitoring_grp_id, chat_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('DELETE FROM public.tb_telegram_managers WHERE monitoring_grp_id = %s AND chat_id = %s;',
                       (monitoring_grp_id, chat_id))
        affected_rows = cursor.rowcount
        conn.commit()
        return affected_rows > 0

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

    return False


def on_all_alarm(monitoring_grp_id, chat_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE public.tb_telegram_managers
            SET notification_on = true
            WHERE monitoring_grp_id = %s AND chat_id = %s;
        ''', (monitoring_grp_id, chat_id))
        affected_rows = cursor.rowcount
        conn.commit()
        return affected_rows > 0

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


def off_all_alarm(monitoring_grp_id, chat_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            UPDATE public.tb_telegram_managers
            SET notification_on = false
            WHERE monitoring_grp_id = %s AND chat_id = %s
        ''', (monitoring_grp_id, chat_id))
        affected_rows = cursor.rowcount
        conn.commit()
        return affected_rows > 0

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
    return False


def get_all_telegram_managers():
    try:
        conn = get_connection()
        if conn is None:
            print("❌ Database connection failed. Check your settings!")  # 오류 로그 출력
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()
        cursor.execute('SELECT * FROM public.tb_telegram_managers')  # 관리자 데이터 조회
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
        return rows

    except Exception as e:
        print(f"❌ Database Query Error: {e}")  # 콘솔에 오류 출력
        return None


# 데이터 수정
def update_or_insert_manager(chat_id, manager_name, department, position, region, notification_on, registered_at,
                             last_alert_at):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # ✅ 현재 UTC 시간 설정
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # ✅ `registered_at`이 없으면 현재 시간으로 설정, 있으면 포맷 변환
        if not registered_at:
            registered_at = current_time
        else:
            registered_at = datetime.strptime(registered_at, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')

        # ✅ `last_alert_at`이 없으면 None 처리
        last_alert_at = None if not last_alert_at else datetime.strptime(last_alert_at,
                                                                         '%Y-%m-%d %H:%M:%S.%f').strftime(
            '%Y-%m-%d %H:%M:%S')

        # ✅ 먼저 chat_id가 존재하는지 확인
        cursor.execute("SELECT COUNT(*) FROM public.tb_telegram_managers WHERE chat_id = %s", (chat_id,))
        result = cursor.fetchone()

        if result and result[0] > 0:
            # ✅ `chat_id`가 존재하면 `UPDATE`
            cursor.execute('''
                UPDATE public.tb_telegram_managers
                SET manager_name = %s, department = %s, position = %s, region = %s, 
                    notification_on = %s, registered_at = %s
                WHERE chat_id = %s
            ''', (manager_name, department, position, region, notification_on, registered_at, chat_id))
            action = "updated"
        else:
            # ✅ `chat_id`가 없으면 `INSERT`
            cursor.execute('''
                INSERT INTO public.tb_telegram_managers 
                (chat_id, manager_name, department, position, region, notification_on, registered_at, last_alert_at) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (chat_id, manager_name, department, position, region, notification_on, registered_at, last_alert_at))
            action = "inserted"

        conn.commit()
        return action

    except Exception as e:
        print(f"❌ Error updating/inserting manager: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


###DT 모델링 API###
# 충돌 이벤트 데이터 반환 함수
def select_collision_events(camera_id=None, limit=10, status=None):
    """
    충돌 관련 이벤트 데이터를 조회하여 반환하는 함수.

    Args:
        camera_id (str, optional): 특정 카메라 ID로 필터링. None이면 모든 카메라 데이터 반환.
        limit (int, optional): 반환할 최대 레코드 수. 기본값은 10.
        status (str, optional): 이벤트 상태로 필터링 ('new', 'processed', 등). None이면 모든 상태 반환.

    Returns:
        dict: 상태 및 충돌 이벤트 데이터를 포함하는 딕셔너리.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        query = '''
            SELECT 
                id, 
                camera_id, 
                timestamp, 
                worker_box, 
                object_class, 
                object_box, 
                alert_type, 
                status, 
                created_at
            FROM public.collision_alert_log
            WHERE 1=1
        '''

        params = []

        # 카메라 ID 필터 추가
        if camera_id:
            query += " AND camera_id = %s"
            params.append(camera_id)

        # 상태 필터 추가
        if status:
            query += " AND status = %s"
            params.append(status)

        # 정렬 및 제한 추가
        query += " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)

        # 결과 가져오기
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        data = [
            {col: (row[idx] if row[idx] is not None else None) for idx, col in enumerate(col_names)}
            for row in rows
        ]

        # 날짜/시간 형식 변환 및 충돌 정보 추가
        collision_prints = []
        for i, item in enumerate(data):
            if 'created_at' in item and item['created_at']:
                item['created_at'] = item['created_at'].strftime('%Y-%m-%d %H:%M:%S')

            # 충돌 관련 정보 추가
            try:
                # 객체 ID는 데이터베이스 ID를 사용
                track_id = item['id']

                # worker_box와 object_box 파싱
                try:
                    worker_box = json.loads(item['worker_box'])
                    object_box = json.loads(item['object_box'])

                    # 바운딩 박스의 중심점 계산
                    worker_center_x = (worker_box[0] + worker_box[2]) / 2
                    worker_center_y = (worker_box[1] + worker_box[3]) / 2

                    object_center_x = (object_box[0] + object_box[2]) / 2
                    object_center_y = (object_box[1] + object_box[3]) / 2

                    # 바운딩 박스의 모서리 좌표 계산
                    worker_corners = [
                        (worker_box[0], worker_box[1]),  # 좌상단
                        (worker_box[2], worker_box[1]),  # 우상단
                        (worker_box[0], worker_box[3]),  # 좌하단
                        (worker_box[2], worker_box[3])   # 우하단
                    ]

                    object_corners = [
                        (object_box[0], object_box[1]),  # 좌상단
                        (object_box[2], object_box[1]),  # 우상단
                        (object_box[0], object_box[3]),  # 좌하단
                        (object_box[2], object_box[3])   # 우하단
                    ]

                    # 모든 모서리 좌표 합치기
                    all_corners = worker_corners + object_corners

                    # 모서리 좌표의 최소/최대 x, y 값 찾기
                    min_x = min([c[0] for c in all_corners])
                    max_x = max([c[0] for c in all_corners])
                    min_y = min([c[1] for c in all_corners])
                    max_y = max([c[1] for c in all_corners])

                    # 중심점의 평균으로 대략적인 그리드 위치 추정
                    center_x = (worker_center_x + object_center_x) / 2
                    center_y = (worker_center_y + object_center_y) / 2

                    # 이미지 크기를 기준으로 그리드 위치 추정 (10x10 그리드 가정)
                    # 실제 그리드 크기에 맞게 조정 필요
                    grid_size = 10
                    row = int(center_y * grid_size / 1080)  # 1080p 이미지 가정
                    col = int(center_x * grid_size / 1920)  # 1080p 이미지 가정

                    # 그리드 범위 제한
                    row = max(0, min(row, grid_size - 1))
                    col = max(0, min(col, grid_size - 1))

                    # cx, ry 계산 (바운딩 박스 크기 기반)
                    width = max_x - min_x
                    height = max_y - min_y

                    # 그리드 셀 크기 기준으로 cx, ry 계산
                    cell_width = 1920 / grid_size  # 그리드 셀의 너비
                    cell_height = 1080 / grid_size  # 그리드 셀의 높이

                    cx = max(1, int(width / cell_width) + 1)
                    ry = max(1, int(height / cell_height) + 1)

                except Exception as e:
                    # 파싱 오류 시 기본값 사용
                    print(f"바운딩 박스 파싱 오류: {e}")
                    row, col = i, i  # 기본값으로 인덱스 사용
                    cx, ry = 1, 1  # 기본값 설정

                # 작업자 정보 출력
                worker_print = f"객체 ID {track_id} (작업자): {row}_{col}"
                collision_prints.append(worker_print)

                # 객체 정보 출력
                class_name_display = item['object_class']
                object_print = f"객체 ID {track_id} ({class_name_display}): {cx}x{ry}"
                collision_prints.append(object_print)

                # 새로운 형식으로 detected_area와 object_size 추가
                item["detected_area"] = f"{row}_{col}"  # grid_key 형식으로 변경
                item["object_size"] = f"{cx}x{ry}"
            except Exception as e:
                print(f"충돌 정보 처리 중 오류: {e}")

        print("🔹 Collision Events Query Executed Successfully")
        print(f"🔹 Found {len(data)} collision events")

        return {"status": "success", "data": data}

    except Exception as e:
        print("🔺 Error:", str(e))
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

# 카메라 그리드 데이터 반환 함수
def select_dt_safety_Modeling_data():
    """
    모든 카메라의 그리드 데이터를 조회하고, width 및 height 값을 변환하여 반환하는 함수.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
        SELECT
            tc.camera_id,
            tcmg.grp_nm,
            tc.camera_nm,
            tcg.grid_unit
        FROM public.tb_camera tc
        LEFT JOIN public.tb_camera_monitoring_layout tcml
            ON tc.camera_id = tcml.camera_id
        LEFT JOIN public.tb_camera_monitoring_grp tcmg
            ON tcml.monitoring_grp_id = tcmg.monitoring_grp_id
        LEFT JOIN public.tb_camera_safety_grid tcg
            ON tc.camera_id = tcg.camera_id
        WHERE tcg.grid_data IS NOT NULL;
        ''')

        # 결과 가져오기
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        data = [
            {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
            for row in rows
        ]

        # 🔹 모든 카메라의 raw grid 데이터 조회
        raw_grid_result = get_raw_safety_grid_coordinates_all_cam()

        # 🔹 Grid 데이터를 변환하여 cols, rows 획득
        camera_grid_data = generate_coordinates_by_cams(raw_grid_result)

        # 🔹 조회된 데이터와 cols, rows 결합
        for camera in data:
            camera_id = camera["camera_id"]
            if camera_id in camera_grid_data:
                camera["grid_width"] = camera_grid_data[camera_id]["cols"]  # cols를 width로 설정
                camera["grid_height"] = camera_grid_data[camera_id]["rows"]  # rows를 height로 설정

        # ✅ 디버깅용 출력
        print("🔹 Query Executed Successfully")
        print("🔹 Final Data:", data)

        return {"status": "success", "data": data}

    except Exception as e:
        print("🔺 Error:", str(e))  # 에러 출력
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

# 카메라 그리드 데이터 반환 함수
def select_dt_Modeling_data():
    """
    모든 카메라의 그리드 데이터를 조회하고, width 및 height 값을 변환하여 반환하는 함수.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
        SELECT
            tc.camera_id,
            tcmg.grp_nm,
            tc.camera_nm,
            tcg.grid_unit
        FROM public.tb_camera tc
        LEFT JOIN public.tb_camera_monitoring_layout tcml
            ON tc.camera_id = tcml.camera_id
        LEFT JOIN public.tb_camera_monitoring_grp tcmg
            ON tcml.monitoring_grp_id = tcmg.monitoring_grp_id
        LEFT JOIN public.tb_camera_grid tcg
            ON tc.camera_id = tcg.camera_id
        WHERE tcg.grid_data IS NOT NULL;
        ''')

        # 결과 가져오기
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        data = [
            {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
            for row in rows
        ]

        # 🔹 모든 카메라의 raw grid 데이터 조회
        raw_grid_result = get_raw_grid_coordinates_all_cam()

        # 🔹 Grid 데이터를 변환하여 cols, rows 획득
        camera_grid_data = generate_coordinates_by_cams(raw_grid_result)

        # 🔹 조회된 데이터와 cols, rows 결합
        for camera in data:
            camera_id = camera["camera_id"]
            if camera_id in camera_grid_data:
                camera["grid_width"] = camera_grid_data[camera_id]["cols"]  # cols를 width로 설정
                camera["grid_height"] = camera_grid_data[camera_id]["rows"]  # rows를 height로 설정

        # ✅ 디버깅용 출력
        print("🔹 Query Executed Successfully")
        print("🔹 Final Data:", data)

        return {"status": "success", "data": data}

    except Exception as e:
        print("🔺 Error:", str(e))  # 에러 출력
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


def select_safety_detected_object_dimensions(camera_id):
    """
    특정 카메라 ID에 대한 탐지된 위험체의 위치 격자 좌표를 반환 합니다.
    """
    cctv = get_camera_by_id(camera_id)

    server_id = cctv[4]
    
    pid = cctv[7]
    if pid == '':
        return {"code": 200, "status": "error", "message": f"No detected objects found for camera_id {camera_id}"}
    
    port_num = cctv[14]

    # print('server_id')
    # print(server_id)
    server_data = get_ai_server(server_id)
    server_ip = server_data[3]
    restapi_port = server_data[4]
    # print('pid')
    # print(pid)
    # print('port_num')
    # print(port_num)

    # POST 요청을 보낼 URL
    url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/get_grid_st_socket'
    print(url)

    # 요청에 포함할 데이터 (JSON 형식)
    data = {
        "port_num": port_num
    }

    
    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    # POST 요청 보내기
    response = requests.post(url, data=json.dumps(data), headers=headers)
    # 응답 처리
    if response.status_code == 200:
        print("POST 요청 성공")
        print("응답 데이터:", response.json())
        # return jsonify({"message": f"stop successfully"})
        out_text = response.json()
        out_text["success"] = True
        out_text["code"] = 200
        out_text["msg"] = "성공하였습니다."
        return jsonify(out_text)
    else:
        print("POST 요청 실패")
        print("상태 코드:", response.status_code)
        print("응답 내용:", response.text)

    return jsonify({"success": False,
                    "code": 404,
                    "msg": f"err"})

def detected_objects(filter_id: int):
    """
    tb_twin_detection_filter.filter_id에 해당하는 필터 조건과 일치하는
    tb_twin_detection 데이터의 image_id, detection_id를 반환.

    - 필터 기준 컬럼:
      camera_id, grid_width, grid_height, detected_row, detected_col, object_label
    - object_label은 detection 쪽이 'BWTS_70%_0' 형식일 수 있으므로
      split_part(d.object_label, '_', 1) = filter.object_label 로 비교
    - 카메라의 '최신 이미지' 1건에 대해서만 검사
    """
    conn = get_connection()
    if conn is None:
        print("❌ Database connection failed.")
        return {"status": "error", "message": "Database connection failed."}

    cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

    try:
        # 1) filter_id로 필터 규칙 조회
        cursor.execute('''
            SELECT
                filter_id,
                camera_id,
                grid_width,
                grid_height,
                detected_row,
                detected_col,
                object_label
            FROM public.tb_twin_detection_filter
            WHERE filter_id = %s
        ''', (filter_id,))
        filter_row = cursor.fetchone()

        if not filter_row:
            return {"status": "error", "message": f"filter_id {filter_id} not found"}

        cam_id      = filter_row["camera_id"]
        f_gw        = filter_row["grid_width"]
        f_gh        = filter_row["grid_height"]
        f_row       = filter_row["detected_row"]
        f_col       = filter_row["detected_col"]
        f_label     = filter_row["object_label"]

        # 2) 해당 카메라의 최신 이미지 1건 찾기
        cursor.execute('''
            SELECT image_id
            FROM public.tb_twin_image
            WHERE camera_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        ''', (cam_id,))
        latest = cursor.fetchone()
        if not latest:
            return {
                "status": "error",
                "message": f"No image found for camera_id {cam_id}"
            }

        latest_image_id = latest["image_id"]

        # 3) 최신 image_id에 대해 필터 조건과 일치하는 detection 찾기
        cursor.execute('''
            SELECT
                d.image_id,
                d.detection_id
            FROM public.tb_twin_detection d
            WHERE d.image_id = %s
              AND d.grid_width   = %s
              AND d.grid_height  = %s
              AND d.detected_row = %s
              AND d.detected_col = %s
              AND d.object_label LIKE %s
        ''', (latest_image_id, f_gw, f_gh, f_row, f_col, f_label + "%"))

        # cursor.execute('''
        #     SELECT
        #         d.image_id,
        #         d.detection_id
        #     FROM public.tb_twin_detection d
        #     WHERE d.image_id = %s
        #       AND d.grid_width   = %s
        #       AND d.grid_height  = %s
        #       AND d.detected_row = %s
        #       AND d.detected_col = %s
        # ''', (latest_image_id, f_gw, f_gh, f_row, f_col))

        rows = cursor.fetchall()

        if not rows:
            return {
                "status": "error",
                "message": f"No detection matched for filter_id {filter_id} on latest image"
            }

        data = [
            {"image_id": r["image_id"], "detection_id": r["detection_id"]}
            for r in rows
        ]

        print(f"🔹 filter_id={filter_id}, camera_id={cam_id}, latest_image_id={latest_image_id}")
        print("🔹 Matched detections:", data)

        return {"status": "success", "data": data}

    except Exception as e:
        print("❌ select_detected_object_dimensions error:", e)
        return {"status": "error", "message": str(e)}

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def select_detected_object_dimensions(camera_id, base_time=None):
    """
    특정 카메라 ID에 대한 탐지된 객체의 크기 및 좌표 정보를 조회합니다.
    tb_twin_detection_filter에 등록된 필터 규칙과 일치하는 detection 결과는 제외합니다.
    object_label은 'BWTS_70%_0' 형식으로 들어와도, '_0'을 제거한 값으로 필터와 비교합니다.
    """
    def normalize_label(label: str | None) -> str | None:
        """
        object_label에서 끝의 '_숫자' 인덱스를 제거해서 비교용으로 사용.
        예) 'BWTS_70%_0' -> 'BWTS_70%'
        """
        if label is None:
            return None
        # 이미 "NULL" 같은 값이면 그대로
        if label == "NULL":
            return label

        # 뒤에서부터 한 번만 split 후, 뒤 조각이 숫자면 제거
        parts = label.rsplit("_", 1)
        if len(parts) == 2 and parts[1].isdigit():
            return parts[0]
        return label

    conn = get_connection()
    if conn is None:
        print("❌ Database connection failed.")
        return {"status": "error", "message": "Database connection failed."}

    cursor = conn.cursor()

    try:
        # -----------------------------
        # 1) detection 정보 조회
        # -----------------------------
        if base_time is None:
            cursor.execute('''
            WITH latest_image AS (
                SELECT image_id, camera_id, created_at
                FROM public.tb_twin_image
                WHERE camera_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                li.camera_id,
                tc.camera_nm,
                d.object_label,
                d.grid_width,
                d.grid_height,
                d.detected_row,
                d.detected_col,
                d.image_id,
                d.detection_id,
                d.order_no,
                COALESCE(bwts.hullno, scrubber.hullno) AS hullno,
                'detection' as type
            FROM latest_image li
            LEFT JOIN public.tb_camera tc
                ON li.camera_id = tc.camera_id
            LEFT JOIN public.tb_twin_detection d
                ON li.image_id = d.image_id
            LEFT JOIN public.dt_leg_ord_data_bwts bwts
                ON bwts.ordnum = split_part(d.order_no, '-', 1)
               AND bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
            LEFT JOIN public.dt_leg_ord_data_scrubber scrubber
                ON scrubber.ordnum = split_part(d.order_no, '-', 1)
               AND scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
            ''', (camera_id,))
        else:
            cursor.execute('''
            WITH latest_image AS (
                SELECT image_id, camera_id, created_at
                FROM public.tb_twin_image
                WHERE camera_id = %s AND created_at <= %s
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                li.camera_id,
                tc.camera_nm,
                d.object_label,
                d.grid_width,
                d.grid_height,
                d.detected_row,
                d.detected_col,
                d.image_id,
                d.detection_id,
                d.order_no,
                COALESCE(bwts.hullno, scrubber.hullno) AS hullno,
                'detection' as type
            FROM latest_image li
            LEFT JOIN public.tb_camera tc
                ON li.camera_id = tc.camera_id
            LEFT JOIN public.tb_twin_detection d
                ON li.image_id = d.image_id
            LEFT JOIN public.dt_leg_ord_data_bwts bwts
                ON bwts.ordnum = split_part(d.order_no, '-', 1)
               AND bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
            LEFT JOIN public.dt_leg_ord_data_scrubber scrubber
                ON scrubber.ordnum = split_part(d.order_no, '-', 1)
               AND scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
            ''', (camera_id, base_time))

        # detection 결과
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        data = [
            {col: (row[idx] if row[idx] is not None else "NULL")
             for idx, col in enumerate(col_names)}
            for row in rows
        ]

        # object_label이 전부 NULL인 한 건만 있는 경우 제거
        if len(data) == 1 and data[0]['object_label'] == "NULL":
            data = []

        # -----------------------------
        # 2) 필터 테이블 조회 후 detection 필터링
        # -----------------------------
        # camera_id 기준으로 필터 규칙 가져오기
        cursor.execute('''
            SELECT camera_id, grid_width, grid_height,
                   detected_row, detected_col, object_label
            FROM public.tb_twin_detection_filter
            WHERE camera_id = %s
        ''', (camera_id,))
        filter_rows = cursor.fetchall()

        filter_set = set()
        for f_row in filter_rows:
            f_cam, f_gw, f_gh, f_dr, f_dc, f_label = f_row
            norm_label = normalize_label(f_label)
            filter_set.add((f_cam, f_gw, f_gh, f_dr, f_dc, norm_label))

        # detection 데이터에서 필터에 걸리는 것 제거
        if filter_set and data:
            filtered_detection = []
            for item in data:
                # type == 'detection'만 필터 대상
                if item.get('type') == 'detection':
                    norm_label = normalize_label(item.get('object_label'))
                    key = (
                        item.get('camera_id'),
                        item.get('grid_width'),
                        item.get('grid_height'),
                        item.get('detected_row'),
                        item.get('detected_col'),
                        norm_label,
                    )
                    # 필터에 존재하면 제외
                    if key in filter_set:
                        continue
                filtered_detection.append(item)
            data = filtered_detection

        # -----------------------------
        # 3) 사용자 입력 정보 조회 (input)
        # -----------------------------
        if base_time is None:
            cursor.execute('''
            WITH latest_image AS (
                SELECT image_id, camera_id, created_at
                FROM public.tb_twin_image
                WHERE camera_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                li.camera_id,
                tc.camera_nm,
                d.object_label,
                d.grid_width,
                d.grid_height,
                d.input_row,
                d.input_col,
                d.image_id,
                d.input_id,
                d.order_no,
                COALESCE(bwts.hullno, scrubber.hullno) AS hullno,
                'input' as type
            FROM latest_image li
            LEFT JOIN public.tb_camera tc
                ON li.camera_id = tc.camera_id
            LEFT JOIN public.tb_twin_user_input d
                ON li.image_id = d.image_id
            LEFT JOIN public.dt_leg_ord_data_bwts bwts
                ON bwts.ordnum = split_part(d.order_no, '-', 1)
               AND bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
            LEFT JOIN public.dt_leg_ord_data_scrubber scrubber
                ON scrubber.ordnum = split_part(d.order_no, '-', 1)
               AND scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
            ''', (camera_id,))
        else:
            cursor.execute('''
            WITH latest_image AS (
                SELECT image_id, camera_id, created_at
                FROM public.tb_twin_image
                WHERE camera_id = %s AND created_at <= %s
                ORDER BY created_at DESC
                LIMIT 1
            )
            SELECT
                li.camera_id,
                tc.camera_nm,
                d.object_label,
                d.grid_width,
                d.grid_height,
                d.input_row,
                d.input_col,
                d.image_id,
                d.input_id,
                d.order_no,
                COALESCE(bwts.hullno, scrubber.hullno) AS hullno,
                'input' as type
            FROM latest_image li
            LEFT JOIN public.tb_camera tc
                ON li.camera_id = tc.camera_id
            LEFT JOIN public.tb_twin_user_input d
                ON li.image_id = d.image_id
            LEFT JOIN public.dt_leg_ord_data_bwts bwts
                ON bwts.ordnum = split_part(d.order_no, '-', 1)
               AND bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
            LEFT JOIN public.dt_leg_ord_data_scrubber scrubber
                ON scrubber.ordnum = split_part(d.order_no, '-', 1)
               AND scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
            ''', (camera_id, base_time))

        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        data1 = [
            {col: (row[idx] if row[idx] is not None else "NULL")
             for idx, col in enumerate(col_names)}
            for row in rows
        ]

        if len(data1) == 1 and data1[0]['object_label'] == "NULL":
            data1 = []

        # detection + input 합치기
        data = data + data1

        if not data:
            return {
                "status": "error",
                "message": f"No detected objects found for camera_id {camera_id}"
            }

        # ✅ 디버깅용 출력
        print(f"🔹 Query executed successfully for camera_id {camera_id}")
        print("🔹 Retrieved Data:", data)

        return {"status": "success", "data": data}

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

def select_detected_object_inventories(sim_id=None, base_time=None):
    """
    특정 카메라 ID에 대한 탐지된 객체의 크기 및 좌표 정보를 조회합니다.
    """
    conn = get_connection()
    if conn is None:
        print("❌ Database connection failed.")
        return {"status": "error", "message": "Database connection failed."}

    cursor = conn.cursor()

    try:
        # 시뮬레이션 모드
        if sim_id == None:
            # 검출 정보
            if base_time == None:
                cursor.execute('''
                select REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '') as label,
                        count(REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_detection ttd 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '');
                ''', ())
            else:
                cursor.execute('''
                select REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '') as label,
                        count(REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_detection ttd 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                            and created_at <= %s
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '');
                ''', ( base_time,))

            # 결과 가져오기
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            # 데이터 변환
            data = [
                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
                for row in rows
            ]

            if len(data) == 1:
                if data[0]['label'] == "NULL":
                    data = []

            # 사용자 입력 정보
            if base_time == None:
                cursor.execute('''
                select REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '') as label,
                        count(REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_user_input ttui 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')
                ''', ())
            else:
                cursor.execute('''
                select REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '') as label,
                        count(REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_user_input ttui 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                            and created_at <= %s
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')
                ''', (base_time,))

            # 결과 가져오기
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            # 데이터 변환
            data1 = [
                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
                for row in rows
            ]
            if len(data1) == 1:
                if data1[0]['label'] == "NULL":
                    data1 = []
            #함치기
            data = data + data1

            # label을 기준으로 count 합산
            aggregated = defaultdict(int)
            for item in data:
                aggregated[item["label"]] += item["count"]
            
            # 결과를 리스트로 재구성
            result = [{"label": label, "count": count} for label, count in aggregated.items()]


            if not result:
                return {"status": "error", "message": f"No detected objects found"}

            # ✅ 디버깅용 출력
            print(f"🔹 Query executed successfully")
            print("🔹 Retrieved Data:", result)

            return {"status": "success", "data": result}
        else:
            # 해당 sim_id가 bwts 또는 scrubber 인지 확인
            query = """
                        select dsm.type_code 
                        from public.dt_sim_master dsm 
                        where dsm.sim_id = %s

            """
            cursor.execute(query, (sim_id,))
            row = cursor.fetchone()
            type_code = row[0]
            print('type_code', type_code)

            if type_code == 'BWTS' or type_code is None:
                query = """
                            select foo.progress_percentage, foo.sim_id, foo.mac_id, foo.mac_name, foo.start_date, foo.end_date, foo.due_date, tc.location
                                from (WITH progress AS (
                                                    SELECT CASE
                                                            WHEN TIMESTAMP %s < d.start_date THEN 0
                                                            WHEN TIMESTAMP %s >= d.end_date THEN 100
                                                            ELSE
                                                                ROUND(
                                                                        ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                                                            EXTRACT(EPOCH FROM d.start_date)) /
                                                                            NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                                                        100
                                                                )
                                                            END AS exact_progress,
                                                        d.sim_id,
                                                        d.mac_id,
                                                        d.start_date,
                                                        d.end_date,
                                                        d.due_date,
                                                        m.mac_name,
                                                        split_part(m.mac_name , '_', 1) as camera_id
                                                    FROM dt_sim_list d
                                                            LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                                                            left join public.tb_camera tc on split_part(m.mac_name , '_', 1) = tc.camera_id
                                                    WHERE d.sim_id = %s
                                                        AND m.mac_name IS NOT NULL
                                                        AND LOWER(m.mac_name) != 'none'
                                                )
                                                SELECT DISTINCT ON (mac_name)
                                                    CASE
                                                        WHEN exact_progress <= 0 THEN 0
                                                        WHEN exact_progress <= 10 THEN 10
                                                        WHEN exact_progress <= 30 THEN 30
                                                        WHEN exact_progress <= 50 THEN 50
                                                        WHEN exact_progress <= 70 THEN 70
                                                        WHEN exact_progress <= 90 THEN 90
                                                        ELSE 100
                                                        END AS progress_percentage,
                                                    sim_id,
                                                    mac_id,
                                                    mac_name,
                                                    start_date,
                                                    end_date,
                                                    due_date,
                                                    camera_id
                                                FROM progress
                                                WHERE due_date >= TIMESTAMP %s 
                                                ORDER BY mac_name, sim_id, mac_id) foo
                                left join public.tb_camera tc on tc.camera_id = foo.camera_id;
                """
                # 현재 시간 또는 제공된 base_time 사용
                current_time = base_time if base_time is not None else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(query, (current_time, current_time, current_time, sim_id, current_time,))

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                result0 = [dict(zip(columns, row)) for row in rows]

                # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
                filtered_results = []

                
                for result in result0:
                    if result.get('mac_name'):
                        mac_name = result['mac_name']
                        # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                        match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                        if match:
                            # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                            temp = {}
                            extracted_camera_id = mac_name.split('_')[0]
                            temp['image_id'] = None
                            temp['object_label'] = 'BWTS_' + str(result['progress_percentage']) + '%'
                            temp['camera_id'] = extracted_camera_id
                            temp['localtion'] = result['location']

                            filtered_results.append(temp)
        
                print('filtered_results', filtered_results)

                # 검출 내역 에서 라벨명 bwts로 시작 되는 항목 제거하고 합치기
                with conn.cursor() as detection_cur:
                    # 최신 이미지 ID 조회
                    detection_cur.execute('''
                    WITH latest_images AS (
                        SELECT sub.image_id
                        FROM (
                            SELECT DISTINCT ON (tti.camera_id) *
                            FROM tb_twin_image tti
                            where tti.created_at <= %s
                            ORDER BY tti.camera_id, tti.created_at DESC
                        ) sub
                    )
                    SELECT ttd.image_id, 
                            CASE 
                                WHEN SPLIT_PART(ttd.object_label, '_', 2) ~ '^[0-9]+$' -- 두 번째 파트가 숫자면 (예: TC_1)
                                THEN SPLIT_PART(ttd.object_label, '_', 1)
                                ELSE SPLIT_PART(ttd.object_label, '_', 1) || '_' || SPLIT_PART(ttd.object_label, '_', 2)
                            END AS object_label
                            , tti.camera_id, tc.location
                    FROM tb_twin_detection ttd
                    left join tb_twin_image tti on ttd.image_id = tti.image_id
                    left join tb_camera tc on tc.camera_id = tti.camera_id
                    WHERE ttd.image_id IN (SELECT image_id FROM latest_images)
                    ''', (current_time, ))

                    # 결과 가져오기
                    detection_rows = detection_cur.fetchall()
                    detection_col_names = [desc[0] for desc in detection_cur.description]

                    print('detection_col_names', detection_col_names)

                    # 데이터 변환
                    detection_data = [
                        {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                        for row in detection_rows
                    ]

                    # "BWTS_" 로 시작하는 object_label 제거
                    detection_data = [d for d in detection_data if not d["object_label"].startswith("BWTS_")]
                    print('detection_data', detection_data)

                    if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                        detection_data = []

                all_data = filtered_results + detection_data
                print('all_data', all_data)

                # object_label 값만 추출
                labels = [d['object_label'] for d in all_data]

                # Counter로 개수 세기
                counts = Counter(labels)

                # 원하는 형식으로 변환
                result = [{"count": v, "label": k} for k, v in counts.items()]

                print(result)
                
                return {"status": "success", "data": result}

    except Exception as e:
        print("🔺 Error:", str(e))  # 에러 출력
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

def select_detected_object_inventories_hoseon(sim_id=None, base_time=None):
    """
    특정 카메라 ID에 대한 탐지된 객체의 크기 및 좌표 정보를 조회합니다.
    """
    conn = get_connection()
    if conn is None:
        print("❌ Database connection failed.")
        return {"status": "error", "message": "Database connection failed."}

    cursor = conn.cursor()

    try:
        # 시뮬레이션 모드
        if sim_id == None:
            # 검출 정보
            if base_time == None:
                cursor.execute('''
                WITH ttd_clean AS (
                    SELECT
                        ttd.*,
                        REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '') AS label_clean,
                        CASE
                            WHEN split_part(ttd.order_no, '-', 2) ~ '^[0-9]+$'
                                THEN split_part(ttd.order_no, '-', 2)::INT
                            ELSE NULL
                        END AS ordseq_int
                    FROM public.tb_twin_detection ttd
                )
                SELECT
                    t.label_clean AS label,
                    COALESCE(d.hullno, s.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                    t.order_no,
                    COUNT(*) AS count
                FROM ttd_clean t
                LEFT JOIN public.dt_leg_ord_data_bwts d
                    ON d.ordnum = split_part(t.order_no, '-', 1)
                    AND d.ordseq = t.ordseq_int
                LEFT JOIN public.dt_leg_ord_data_scrubber s
                    ON s.ordnum = split_part(t.order_no, '-', 1)
                    AND s.ordseq = t.ordseq_int
                WHERE t.image_id IN (
                    SELECT DISTINCT ON (camera_id) image_id
                    FROM public.tb_twin_image tti
                    WHERE camera_id IN (SELECT camera_id FROM public.tb_camera_grid)
                    ORDER BY camera_id ASC, tti.image_id DESC
                )
                GROUP BY t.label_clean, d.hullno, s.hullno, t.order_no;
                ''', ())
            else:
                cursor.execute('''
                WITH ttd_clean AS (
                    SELECT
                        ttd.*,
                        REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '') AS label_clean,
                        CASE
                            WHEN split_part(ttd.order_no, '-', 2) ~ '^[0-9]+$'
                                THEN split_part(ttd.order_no, '-', 2)::INT
                            ELSE NULL
                        END AS ordseq_int
                    FROM public.tb_twin_detection ttd
                )
                SELECT
                    t.label_clean AS label,
                    COALESCE(d.hullno, s.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                    t.order_no,
                    COUNT(*) AS count
                FROM ttd_clean t
                LEFT JOIN public.dt_leg_ord_data_bwts d
                    ON d.ordnum = split_part(t.order_no, '-', 1)
                    AND d.ordseq = t.ordseq_int
                LEFT JOIN public.dt_leg_ord_data_scrubber s
                    ON s.ordnum = split_part(t.order_no, '-', 1)
                    AND s.ordseq = t.ordseq_int
                WHERE t.image_id IN (
                    SELECT DISTINCT ON (camera_id) image_id
                    FROM public.tb_twin_image tti
                    WHERE camera_id IN (SELECT camera_id FROM public.tb_camera_grid)
                        and created_at <= %s
                    ORDER BY camera_id ASC, tti.image_id DESC
                )
                GROUP BY t.label_clean, d.hullno, s.hullno, t.order_no;
                ''', ( base_time,))

            # 결과 가져오기
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            # 데이터 변환
            data = [
                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
                for row in rows
            ]
            print('data:::::', data)
            if len(data) == 1:
                if data[0]['label'] == "NULL":
                    data = []

            # 사용자 입력 정보
            if base_time == None:
                cursor.execute('''
                select REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '') as label,
                        ttui.order_no,
                        count(REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_user_input ttui 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', ''), ttui.order_no;
                ''', ())
            else:
                cursor.execute('''
                select REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '') as label,
                        ttui.order_no,
                        count(REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', '')) as count
                from public.tb_twin_user_input ttui 
                where image_id in (select distinct on (camera_id) image_id
                                        from public.tb_twin_image tti
                                        where camera_id in (select camera_id 
                                                                from public.tb_camera_grid tcg )
                                            and created_at <= %s
                                        order by camera_id asc,  tti.image_id desc)
                group by  REGEXP_REPLACE(ttui.object_label, '_[0-9]+$', ''), ttui.order_no;
                ''', (base_time,))

            # 결과 가져오기
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            # 데이터 변환
            data1 = [
                {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
                for row in rows
            ]
            if len(data1) == 1:
                if data1[0]['label'] == "NULL":
                    data1 = []
            #함치기
            data = data + data1

            aggregated = defaultdict(int)

            for item in data:
                # key = (item["label"], item["order_no"], item["hullno"])  # label, order_no 기준 그룹핑
                label = item.get("label", "")  # label 없을 때도 대비
                order_no = _norm(item.get("order_no"))
                # hullno / hull_no 혼용 대비, 없으면 공란
                hullno = _norm(item.get("hullno", item.get("hull_no")))
                key = (label, order_no, hullno)
                aggregated[key] += item["count"]

            # 결과를 리스트로 재구성
            result = [
                {"label": label, "order_no": order_no, "count": count, "hullno": hullno }
                for (label, order_no, hullno), count in aggregated.items()
            ]

            # 보기 좋게 정렬 (선택)
            result.sort(key=lambda x: (x["label"], x["order_no"]))

            print(result)



            if not result:
                return {"status": "error", "message": f"No detected objects found"}

            # ✅ 디버깅용 출력
            print(f"🔹 Query executed successfully")
            print("🔹 Retrieved Data:", result)

            return {"status": "success", "data": result}
        else:
            # 해당 sim_id가 bwts 또는 scrubber 인지 확인
            query = """
                        select dsm.type_code 
                        from public.dt_sim_master dsm 
                        where dsm.sim_id = %s

            """
            cursor.execute(query, (sim_id,))
            row = cursor.fetchone()
            type_code = row[0]
            print('type_code', type_code)

            if type_code == 'BWTS' or type_code is None:
                query = """
                            select foo.progress_percentage, foo.sim_id, foo.mac_id, foo.mac_name, foo.job_name, foo.start_date, foo.end_date, foo.due_date, tc.location
                                from (WITH progress AS (
                                                    SELECT CASE
                                                            WHEN TIMESTAMP %s < d.start_date THEN 0
                                                            WHEN TIMESTAMP %s >= d.end_date THEN 100
                                                            ELSE
                                                                ROUND(
                                                                        ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                                                            EXTRACT(EPOCH FROM d.start_date)) /
                                                                            NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                                                        100
                                                                )
                                                            END AS exact_progress,
                                                        d.sim_id,
                                                        d.mac_id,
                                                        d.start_date,
                                                        d.end_date,
                                                        d.due_date,
                                                        m.mac_name,
                                                        j.job_name,
                                                        split_part(m.mac_name , '_', 1) as camera_id
                                                    FROM dt_sim_list d
                                                            LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                                                            left join dt_sim_job j on d.job_id = j.job_id and d.sim_id = j.sim_id
                                                            left join public.tb_camera tc on split_part(m.mac_name , '_', 1) = tc.camera_id
                                                    WHERE d.sim_id = %s
                                                        AND m.mac_name IS NOT NULL
                                                        AND LOWER(m.mac_name) != 'none'
                                                )
                                                SELECT DISTINCT ON (mac_name)
                                                    CASE
                                                        WHEN exact_progress <= 0 THEN 0
                                                        WHEN exact_progress <= 10 THEN 10
                                                        WHEN exact_progress <= 30 THEN 30
                                                        WHEN exact_progress <= 50 THEN 50
                                                        WHEN exact_progress <= 70 THEN 70
                                                        WHEN exact_progress <= 90 THEN 90
                                                        ELSE 100
                                                        END AS progress_percentage,
                                                    sim_id,
                                                    mac_id,
                                                    mac_name,
                                                    job_name,
                                                    start_date,
                                                    end_date,
                                                    due_date,
                                                    camera_id
                                                FROM progress
                                                WHERE due_date >= TIMESTAMP %s 
                                                ORDER BY mac_name, sim_id, mac_id) foo
                                left join public.tb_camera tc on tc.camera_id = foo.camera_id;
                """
                # 현재 시간 또는 제공된 base_time 사용
                current_time = base_time if base_time is not None else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(query, (current_time, current_time, current_time, sim_id, current_time,))

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                result0 = [dict(zip(columns, row)) for row in rows]

                # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
                filtered_results = []

                
                for result in result0:
                    if result.get('mac_name'):
                        mac_name = result['mac_name']
                        # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                        match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                        if match:
                            # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                            temp = {}
                            extracted_camera_id = mac_name.split('_')[0]
                            temp['image_id'] = None
                            temp['object_label'] = 'BWTS_' + str(result['progress_percentage']) + '%'
                            temp['camera_id'] = extracted_camera_id
                            temp['localtion'] = result['location']
                            temp['hullno'] = result['job_name']

                            filtered_results.append(temp)
        
                print('filtered_results', filtered_results)

                # 검출 내역 에서 라벨명 bwts로 시작 되는 항목 제거하고 합치기
                with conn.cursor() as detection_cur:
                    # 최신 이미지 ID 조회
                    detection_cur.execute('''
                    WITH ttd_clean AS (
                        SELECT
                            ttd.*,
                            REGEXP_REPLACE(ttd.object_label, '_[0-9]+$', '') AS label_clean,
                            CASE
                                WHEN split_part(ttd.order_no, '-', 2) ~ '^[0-9]+$'
                                    THEN split_part(ttd.order_no, '-', 2)::INT
                                ELSE NULL
                            END AS ordseq_int
                        FROM public.tb_twin_detection ttd
                    )
                    SELECT
                        t.label_clean AS object_label,
                        COALESCE(d.hullno, s.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                        t.order_no,                  
                        COUNT(*) AS count
                    FROM ttd_clean t
                    LEFT JOIN public.dt_leg_ord_data_bwts d
                        ON d.ordnum = split_part(t.order_no, '-', 1)
                        AND d.ordseq = t.ordseq_int
                    LEFT JOIN public.dt_leg_ord_data_scrubber s
                        ON s.ordnum = split_part(t.order_no, '-', 1)
                        AND s.ordseq = t.ordseq_int
                    WHERE t.image_id IN (
                        SELECT DISTINCT ON (camera_id) image_id
                        FROM public.tb_twin_image tti
                        WHERE camera_id IN (SELECT camera_id FROM public.tb_camera_grid)
                            and created_at <= %s
                        ORDER BY camera_id ASC, tti.image_id DESC
                    )
                    GROUP BY t.label_clean, d.hullno, s.hullno, t.order_no;
                    ''', (current_time, ))

                    # 결과 가져오기
                    detection_rows = detection_cur.fetchall()
                    detection_col_names = [desc[0] for desc in detection_cur.description]

                    print('detection_col_names', detection_col_names)

                    # 데이터 변환
                    detection_data = [
                        {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                        for row in detection_rows
                    ]

                    # "BWTS_" 로 시작하는 object_label 제거
                    detection_data = [d for d in detection_data if not d["object_label"].startswith("BWTS_")]
                    print('detection_data', detection_data)

                    if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                        detection_data = []

                all_data = filtered_results + detection_data
                print('all_data', all_data)

                # (label, order_no) 조합별로 개수 세기
                grouped = defaultdict(int)
                for item in all_data:
                    label = item.get("object_label", "")
                    hullno = item.get("hullno", "")
                    order_no = item.get("order_no", "")
                    # ✅ 'NULL' 또는 None → 공란("")으로 통일
                    if hullno is None or str(hullno).upper() == "NULL":
                        hullno = ""
                        
                    grouped[(label, hullno, order_no)] += 1

                # 원하는 형식으로 변환
                result = [
                    {"count": count, "label": label, "hullno": hullno, "order_no": order_no}
                    for (label, hullno, order_no), count in grouped.items()
                ]

                print(result)
                
                return {"status": "success", "data": result}

    except Exception as e:
        print("🔺 Error:", str(e))  # 에러 출력
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

def subtract_label_counts(result_data, filter_data):
    """
    result_data: [{'label':..., 'order_no':..., 'count':..., 'hullno':...}, ...]
    filter_data: [{'object_label':'SCRUBBER_100%', 'count':2}, ...]

    동일 label(object_label)의 count를 전체에서 차감해준다.
    우선순위:
      1) hullno == "" (미지정) 먼저 차감
      2) 부족하면 hullno 있는 레코드에서 차감
    """

    # filter_data → dict로 변환 {label: count}
    subtract_map = {}
    for item in filter_data:
        label = item["object_label"]
        cnt = item["count"]
        subtract_map[label] = subtract_map.get(label, 0) + cnt

    # 결과 복사 (원본 유지)
    result = [dict(x) for x in result_data]

    # label 기준으로 result를 그룹화
    from collections import defaultdict
    grouped = defaultdict(list)
    for idx, item in enumerate(result):
        grouped[item["label"]].append((idx, item))

    # 실제 차감 처리
    for label, sub_count in subtract_map.items():
        if label not in grouped:
            continue

        # (1) hullno 없는 항목 먼저
        no_hull = [(idx, it) for idx, it in grouped[label] if _norm(it["hullno"]) == ""]
        # (2) hullno 있는 항목
        yes_hull = [(idx, it) for idx, it in grouped[label] if _norm(it["hullno"]) != ""]

        # 차감 함수
        def deduct(list_items, need_to_sub):
            """list_items: [(idx, item), ...]"""
            remaining = need_to_sub
            for idx, it in list_items:
                if remaining <= 0:
                    break
                c = it["count"]
                if c > remaining:
                    it["count"] = c - remaining
                    remaining = 0
                else:
                    it["count"] = 0
                    remaining -= c
            return remaining

        # hullno 없는 항목에서 먼저 차감
        remain = deduct(no_hull, sub_count)

        # 부족하면 hullno 있는 항목에서 차감
        if remain > 0:
            deduct(yes_hull, remain)

    # count==0 인 항목 제거
    result = [it for it in result if it["count"] > 0]

    return result


def _norm(val):
    """기존 코드와 호환되는 값 정규화"""
    if val is None:
        return ""
    s = str(val).strip()
    if s.upper() == "NULL":
        return ""
    return s


def get_filter_label_counts():
    """
    tb_twin_detection_filter 테이블에서
    object_label별 count(*) 를 반환하는 함수.
    
    반환 형식:
    {
        "status": "success",
        "data": [
            {"object_label": "BWTS_70%", "count": 3},
            {"object_label": "FILTER UNIT", "count": 2},
            ...
        ]
    }
    """
    conn = get_connection()
    if conn is None:
        return {"status": "error", "message": "DB 연결 실패"}

    try:
        cur = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT f.object_label, COUNT(*) AS count
            FROM public.tb_twin_detection_filter f
            GROUP BY f.object_label
            ORDER BY f.object_label;
        """

        cur.execute(query)
        rows = cur.fetchall()

        # 변환 (이미 RealDictCursor라서 dict 형태임)
        data = [
            {
                "object_label": row["object_label"],
                "count": row["count"]
            }
            for row in rows
        ]

        return {"status": "success", "data": data}

    except Exception as e:
        print("❌ get_filter_label_counts ERROR:", e)
        return {"status": "error", "message": str(e)}

    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

def return_selected_object_imgs(image_id, detection_id):
    try:
        conn = get_connection()
        if conn is None:
            return {"status": "error", "message": "Database connection failed."}

        cursor = conn.cursor()
        query = "SELECT base64_image FROM public.tb_twin_detection WHERE image_id = %s and detection_id = %s"
        cursor.execute(query, (image_id, detection_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

        if result:
            image_rel_path = result[0]

            # Flask 프로젝트 루트를 기준으로 경로 연결
            base_dir = current_app.root_path  # 여기서 base_dir은 ai_server/
            full_path = os.path.abspath(os.path.join(base_dir, image_rel_path))

            print(f"[DEBUG] Full path: {full_path}")
            print("[DEBUG] Exists?", os.path.exists(full_path))

            if os.path.exists(full_path):
                with open(full_path, "rb") as image_file:
                    encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
                    return {"status": "success", "image_base64": encoded_string}
            else:
                return {"status": "error", "message": f"Image file not found at path: {image_rel_path}"}
        else:
            return {"status": "error", "message": f"No image found for image_id: {image_id}"}

    except Exception as e:
        return {"status": "error", "message": f"Exception occurred: {str(e)}"}


def insert_user_input_data(data, next_input_id):
    insert_sql = """
        INSERT INTO tb_twin_user_input (
            image_id, input_id, object_label, grid_width, grid_height,
            input_row, input_col, order_no, created_at, created_by,
            updated_at, updated_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s);
    """

    params = (
        data['image_id'],
        next_input_id,
        data['object_label'],
        data['grid_width'],
        data['grid_height'],
        data['input_row'],
        data['input_col'],
        data['order_no'],
        data['created_at'],
        data['created_by'],
        data['updated_by']
    )

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, params)
    finally:
        cur.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

# ✅ input_id 자동 생성 함수
def get_next_input_id(image_id):
    query = "SELECT COALESCE(MAX(input_id), 0) + 1 FROM tb_twin_user_input WHERE image_id = %s"

    conn = get_connection()
    if conn is None:
        return {"status": "error", "message": "Database connection failed."}
    try:
        with conn.cursor() as cur:
            cur.execute(query, (image_id,))
            result = cur.fetchone()
            return result[0]
    finally:
        cur.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


# ✅ input_id 자동 생성 함수
def get_new_image_id(camera_id):
    insert_query = """
            INSERT INTO public.tb_twin_image (camera_id)
            VALUES (%s)
            RETURNING image_id;
            """
    conn = get_connection()
    if conn is None:
        return {"status": "error", "message": "Database connection failed."}
    try:
        with conn.cursor() as cur:
           with conn.cursor() as cur:
            cur.execute(insert_query, (camera_id,))
            image_id = cur.fetchone()[0]
            conn.commit()
            return image_id

    finally:
        cur.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


def select_user_input_data(image_id):
    """
    해당 이미지 아이디의 사용자 입력 객체 정보를 반환 합니다.
    """
    try:
        conn = get_connection()
        if conn is None:
            print("❌ Database connection failed.")
            return {"status": "error", "message": "Database connection failed."}

        cursor = conn.cursor()


        cursor.execute("SELECT * FROM tb_twin_user_input WHERE image_id = %s ORDER BY input_id", (image_id,))

        # 결과 가져오기
        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]

        # 데이터 변환
        data = [
            {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(col_names)}
            for row in rows
        ]

        if len(data) == 1:
            if data[0]['object_label'] == "NULL":
                data = []

        if not data:
            return {"status": "error", "message": f"No user_input_data objects found for image_id {image_id}"}

        # ✅ 디버깅용 출력
        print(f"🔹 Query executed successfully for image_id {image_id}")
        print("🔹 Retrieved Data:", data)

        return {"status": "success", "data": data}

    except Exception as e:
        print("🔺 Error:", str(e))  # 에러 출력
        return {"status": "error", "message": str(e)}

    finally:
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


def updateUserInputData(image_id, input_id, data):
    update_query = """
       UPDATE tb_twin_user_input
            SET
                object_label = %s,
                input_row = %s,
                input_col = %s,
                grid_width = %s,
                grid_height = %s,
                order_no = %s,
                updated_by = %s,
                updated_at = NOW()
            WHERE image_id = %s AND
                  input_id = %s
    """


    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(update_query, (
                data.get("object_label"),
                data.get("input_row"),
                data.get("input_col"),
                data.get("grid_width"),
                data.get("grid_height"),
                data.get("order_no"),
                data.get("updated_by"),
                image_id,
                input_id,
                ))
                conn.commit()
                if cur.rowcount == 0:
                    return 0
            return 1
    finally:
        cur.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")


def DeleteUserInputData(image_id, input_id):
    update_query = """
       DELETE FROM tb_twin_user_input WHERE image_id = %s and input_id = %s
    """
    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(update_query, (
                    image_id,
                    input_id,
                ))
                conn.commit()
                if cur.rowcount == 0:
                    return 0
            return 1
    finally:
        cur.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")



def get_sim_progress_scrubber(sim_id, base_time, camera_id=None):
    """
    시뮬레이션의 진행률을 계산하여 반환합니다.
    mac_name이 중복일 경우 하나만 반환합니다.
    mac_name이 null, None 또는 'none'(대소문자 무관)인 경우 반환하지 않습니다.
    mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출하여 반환합니다.
    camera_id에 해당하는 탐지된 객체 정보도 함께 반환합니다.
    sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.

    Args:
        sim_id (int): 시뮬레이션 ID
        base_time (str): 기준 날짜 (YYYY-MM-DD HH:MM:SS 형식)
        camera_id (str, optional): 카메라 ID로 필터링. 기본값은 None.

    Returns:
        dict or list: 진행률 정보 (progress_percentage, sim_id, mac_id, mac_name, start_date, end_date)
                     mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환되며, camera_id, row_coord, col_coord, width, height 값도 포함됩니다.
                     camera_id에 해당하는 탐지된 객체 정보도 data 키에 포함됩니다.
                     sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 특정 시뮬레이션 ID에 대한 진행률 조회
            query = """
            WITH progress AS (
                SELECT CASE
                        WHEN TIMESTAMP %s < d.start_date THEN 0
                        WHEN TIMESTAMP %s >= d.end_date THEN 100
                        ELSE
                            ROUND(
                                    ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                        EXTRACT(EPOCH FROM d.start_date)) /
                                        NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                    100
                            )
                        END AS exact_progress,
                    d.sim_id,
                    d.mac_id,
                    d.start_date,
                    d.end_date,
                    d.due_date,
                    m.mac_name,
                    j.job_name
                FROM dt_sim_list d
                        LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                        LEFT JOIN dt_sim_job j ON d.job_id = j.job_id AND d.sim_id = j.sim_id
                WHERE d.sim_id = %s
                AND m.mac_name IS NOT NULL
                AND LOWER(m.mac_name) != 'none'
            )
            SELECT DISTINCT ON (mac_name)
                CASE
                    WHEN exact_progress <= 0 THEN 0
                    WHEN exact_progress <= 10 THEN 10
                    WHEN exact_progress <= 30 THEN 30
                    WHEN exact_progress <= 70 THEN 70
                    WHEN exact_progress <= 90 THEN 90
                    ELSE 100
                    END AS progress_percentage,
                sim_id,
                mac_id,
                mac_name,
                job_name,
                start_date,
                end_date,
                due_date
            FROM progress
            WHERE due_date >= TIMESTAMP %s
            ORDER BY mac_name, sim_id, mac_id;
            """
            cur.execute(query, (base_time, base_time, base_time, sim_id, base_time))
            rows = cur.fetchall()

            if not rows:
                return {"error": f"No simulation found with sim_id {sim_id}"}

            columns = [desc[0] for desc in cur.description]
            results = [dict(zip(columns, row)) for row in rows]

            
            # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
            filtered_results = []
            #데이터 포멧
            # {
            #     "camera_id": "CAM0067",
            #     "camera_nm": "B6",
            #     "detected_col": 0,
            #     "detected_row": 1,
            #     "detection_id": 1,
            #     "grid_height": 4,
            #     "grid_width": 4,
            #     "image_id": 319345,
            #     "object_label": "PANEL_0",
            #     "order_no": "NULL",
            #     "type": "detection"
            # },

            for result in results:
                if result.get('mac_name'):
                    mac_name = result['mac_name']
                    # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                    match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                    if match:
                        # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                        extracted_camera_id = mac_name.split('_')[0]
                        result['image_id'] = None
                        result['camera_id'] = extracted_camera_id
                        result['object_label'] = 'SCRUBBER_' + str(result['progress_percentage']) + '%'
                        result['detected_row'] = int(match.group(1))
                        result['detected_col'] = int(match.group(2))
                        result['hullno'] = result['job_name']
                        result['grid_width'] = int(match.group(3))
                        result['grid_height'] = int(match.group(4))
                        result['type'] = 'simulation'

                        # 특정 camera_id로 필터링이 요청된 경우, 해당 camera_id만 포함
                        if camera_id and extracted_camera_id != camera_id:
                            continue

                        filtered_results.append(result)

                        

                        #     # 탐지 정보와 사용자 입력 정보 합치기
                        #     all_data = detection_data + input_data

                        #     # progress_percentage 값을 data 배열의 각 항목에 추가
                        #     for item in all_data:
                        #         item['progress_percentage'] = result['progress_percentage']

                        #     # 결과에 추가
                        #     result['data'] = all_data

            
            print('filtered_results', filtered_results)

            # 검출 내역 에서 라벨명 SCRUBBER로 시작 되는 항목 제거하고 합치기
            with conn.cursor() as detection_cur:
                # 최신 이미지 ID 조회
                detection_cur.execute('''
                WITH latest_image AS (
                    SELECT image_id, camera_id, created_at
                    FROM public.tb_twin_image
                    WHERE camera_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                SELECT
                    li.camera_id,
                    tc.camera_nm,
                    d.object_label,
                    d.grid_width,
                    d.grid_height,
                    d.detected_row,
                    d.detected_col,
                    d.image_id,
                    d.detection_id,
                    d.order_no,
                     COALESCE(bwts.hullno, scrubber.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                    'detection' as type
                FROM latest_image li
                LEFT JOIN public.tb_camera tc
                    ON li.camera_id = tc.camera_id
                LEFT JOIN public.tb_twin_detection d
                    ON li.image_id = d.image_id
                left join public.dt_leg_ord_data_bwts bwts
                    on  bwts.ordnum =  split_part(d.order_no, '-', 1) and bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
                left join public.dt_leg_ord_data_scrubber scrubber
                    on  scrubber.ordnum =  split_part(d.order_no, '-', 1) and scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
                ''', (camera_id,))

                # 결과 가져오기
                detection_rows = detection_cur.fetchall()
                detection_col_names = [desc[0] for desc in detection_cur.description]

                print('detection_col_names', detection_col_names)

                # 데이터 변환
                detection_data = [
                    {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                    for row in detection_rows
                ]

                # "SCRUBBER_" 로 시작하는 object_label 제거
                detection_data = [d for d in detection_data if not d["object_label"].startswith("SCRUBBER_")]
                print('detection_data', detection_data)

                if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                    detection_data = []


            # 사용자 입력 자료 에서 라벨명 SCRUBBER로 시작되는 항목 제거하고 합치기
            with conn.cursor() as detection_cur:
                detection_cur.execute('''
                    WITH latest_image AS (
                        SELECT image_id, camera_id, created_at
                        FROM public.tb_twin_image
                        WHERE camera_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    SELECT
                        li.camera_id,
                        tc.camera_nm,
                        d.object_label,
                        d.grid_width,
                        d.grid_height,
                        d.input_row,
                        d.input_col,
                        d.image_id,
                        d.input_id,
                        d.order_no,
                        COALESCE(bwts.hullno, scrubber.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                        'input' as type
                    FROM latest_image li
                    LEFT JOIN public.tb_camera tc
                        ON li.camera_id = tc.camera_id
                    LEFT JOIN public.tb_twin_user_input d
                        ON li.image_id = d.image_id
                    left join public.dt_leg_ord_data_bwts bwts
                        on  bwts.ordnum =  split_part(d.order_no, '-', 1) and bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
                    left join public.dt_leg_ord_data_scrubber scrubber
                        on  scrubber.ordnum =  split_part(d.order_no, '-', 1) and scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
                    ''', (camera_id,))

                # 결과 가져오기
                input_rows = detection_cur.fetchall()
                input_col_names = [desc[0] for desc in detection_cur.description]

                # 데이터 변환
                input_data = [
                    {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(input_col_names)}
                    for row in input_rows
                ]

                input_data = [d for d in input_data if not d["object_label"].startswith("SCRUBBER_")]
                print('input_data', input_data)

                if len(input_data) == 1 and input_data[0]['object_label'] == "NULL":
                    input_data = []

            all_data = filtered_results + detection_data + input_data

            return all_data

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return {"error": str(e)}

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def get_sim_progress_bwts(sim_id, base_time, camera_id=None):
    """
    시뮬레이션의 진행률을 계산하여 반환합니다.
    mac_name이 중복일 경우 하나만 반환합니다.
    mac_name이 null, None 또는 'none'(대소문자 무관)인 경우 반환하지 않습니다.
    mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출하여 반환합니다.
    camera_id에 해당하는 탐지된 객체 정보도 함께 반환합니다.
    sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.

    Args:
        sim_id (int): 시뮬레이션 ID
        base_time (str): 기준 날짜 (YYYY-MM-DD HH:MM:SS 형식)
        camera_id (str, optional): 카메라 ID로 필터링. 기본값은 None.

    Returns:
        dict or list: 진행률 정보 (progress_percentage, sim_id, mac_id, mac_name, start_date, end_date)
                     mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환되며, camera_id, row_coord, col_coord, width, height 값도 포함됩니다.
                     camera_id에 해당하는 탐지된 객체 정보도 data 키에 포함됩니다.
                     sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # 특정 시뮬레이션 ID에 대한 진행률 조회
            query = """
            WITH progress AS (
                SELECT CASE
                        WHEN TIMESTAMP %s < d.start_date THEN 0
                        WHEN TIMESTAMP %s >= d.end_date THEN 100
                        ELSE
                            ROUND(
                                    ((EXTRACT(EPOCH FROM TIMESTAMP %s) -
                                        EXTRACT(EPOCH FROM d.start_date)) /
                                        NULLIF((EXTRACT(EPOCH FROM d.end_date) - EXTRACT(EPOCH FROM d.start_date)), 0)) *
                                    100
                            )
                        END AS exact_progress,
                    d.sim_id,
                    d.mac_id,
                    d.start_date,
                    d.end_date,
                    d.due_date,
                    m.mac_name,
                    j.job_name
                FROM dt_sim_list d
                        LEFT JOIN dt_sim_mac m ON d.mac_id = m.mac_id AND d.sim_id = m.sim_id
                        LEFT JOIN dt_sim_job j ON d.job_id = j.job_id AND d.sim_id = j.sim_id
                WHERE d.sim_id = %s
                AND m.mac_name IS NOT NULL
                AND LOWER(m.mac_name) != 'none'
            )
            SELECT DISTINCT ON (mac_name)
                CASE
                    WHEN exact_progress <= 0 THEN 0
                    WHEN exact_progress <= 10 THEN 10
                    WHEN exact_progress <= 30 THEN 30
                    WHEN exact_progress <= 50 THEN 50
                    WHEN exact_progress <= 70 THEN 70
                    WHEN exact_progress <= 90 THEN 90
                    ELSE 100
                    END AS progress_percentage,
                sim_id,
                mac_id,
                mac_name,
                job_name,
                start_date,
                end_date,
                due_date
            FROM progress
            WHERE due_date >= TIMESTAMP %s
            ORDER BY mac_name, sim_id, mac_id;
            """
            cur.execute(query, (base_time, base_time, base_time, sim_id, base_time))
            rows = cur.fetchall()

            if not rows:
                return {"error": f"No simulation found with sim_id {sim_id}"}

            columns = [desc[0] for desc in cur.description]
            results = [dict(zip(columns, row)) for row in rows]

            
            # mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출
            filtered_results = []
            #데이터 포멧
            # {
            #     "camera_id": "CAM0067",
            #     "camera_nm": "B6",
            #     "detected_col": 0,
            #     "detected_row": 1,
            #     "detection_id": 1,
            #     "grid_height": 4,
            #     "grid_width": 4,
            #     "image_id": 319345,
            #     "object_label": "PANEL_0",
            #     "order_no": "NULL",
            #     "type": "detection"
            # },

            for result in results:
                if result.get('mac_name'):
                    mac_name = result['mac_name']
                    # 정규식을 사용하여 R{숫자}C{숫자}_{숫자}x{숫자} 패턴 추출
                    match = re.search(r'_R(\d+)C(\d+)_(\d+)x(\d+)', mac_name)
                    if match:
                        # camera_id 추출 (첫 번째 '_' 이전의 문자열)
                        extracted_camera_id = mac_name.split('_')[0]
                        result['image_id'] = None
                        result['camera_id'] = extracted_camera_id
                        result['object_label'] = 'BWTS_' + str(result['progress_percentage']) + '%'
                        result['detected_row'] = int(match.group(1))
                        result['detected_col'] = int(match.group(2))
                        result['hullno'] = result['job_name']
                        result['grid_width'] = int(match.group(3))
                        result['grid_height'] = int(match.group(4))
                        result['type'] = 'simulation'

                        # 특정 camera_id로 필터링이 요청된 경우, 해당 camera_id만 포함
                        if camera_id and extracted_camera_id != camera_id:
                            continue

                        filtered_results.append(result)

                        

                        #     # 탐지 정보와 사용자 입력 정보 합치기
                        #     all_data = detection_data + input_data

                        #     # progress_percentage 값을 data 배열의 각 항목에 추가
                        #     for item in all_data:
                        #         item['progress_percentage'] = result['progress_percentage']

                        #     # 결과에 추가
                        #     result['data'] = all_data

            
            print('filtered_results', filtered_results)

            # 검출 내역 에서 라벨명 bwts로 시작 되는 항목 제거하고 합치기
            with conn.cursor() as detection_cur:
                # 최신 이미지 ID 조회
                detection_cur.execute('''
                WITH latest_image AS (
                    SELECT image_id, camera_id, created_at
                    FROM public.tb_twin_image
                    WHERE camera_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                SELECT
                    li.camera_id,
                    tc.camera_nm,
                    d.object_label,
                    d.grid_width,
                    d.grid_height,
                    d.detected_row,
                    d.detected_col,
                    d.image_id,
                    d.detection_id,
                    d.order_no,
                     COALESCE(bwts.hullno, scrubber.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                    'detection' as type
                FROM latest_image li
                LEFT JOIN public.tb_camera tc
                    ON li.camera_id = tc.camera_id
                LEFT JOIN public.tb_twin_detection d
                    ON li.image_id = d.image_id
                left join public.dt_leg_ord_data_bwts bwts
                    on  bwts.ordnum =  split_part(d.order_no, '-', 1) and bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
                left join public.dt_leg_ord_data_scrubber scrubber
                    on  scrubber.ordnum =  split_part(d.order_no, '-', 1) and scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
                ''', (camera_id,))

                # 결과 가져오기
                detection_rows = detection_cur.fetchall()
                detection_col_names = [desc[0] for desc in detection_cur.description]

                print('detection_col_names', detection_col_names)

                # 데이터 변환
                detection_data = [
                    {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(detection_col_names)}
                    for row in detection_rows
                ]

                # "BWTS_" 로 시작하는 object_label 제거
                detection_data = [d for d in detection_data if not d["object_label"].startswith("BWTS_")]
                print('detection_data', detection_data)

                if len(detection_data) == 1 and detection_data[0]['object_label'] == "NULL":
                    detection_data = []


            # 사용자 입력 자료 에서 라벨명 bwts로 시작되는 항목 제거하고 합치기
            with conn.cursor() as detection_cur:
                detection_cur.execute('''
                    WITH latest_image AS (
                        SELECT image_id, camera_id, created_at
                        FROM public.tb_twin_image
                        WHERE camera_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                    )
                    SELECT
                        li.camera_id,
                        tc.camera_nm,
                        d.object_label,
                        d.grid_width,
                        d.grid_height,
                        d.input_row,
                        d.input_col,
                        d.image_id,
                        d.input_id,
                        d.order_no,
                        COALESCE(bwts.hullno, scrubber.hullno) AS hullno,  -- 둘 중 null 아닌 값 우선 표시
                        'input' as type
                    FROM latest_image li
                    LEFT JOIN public.tb_camera tc
                        ON li.camera_id = tc.camera_id
                    LEFT JOIN public.tb_twin_user_input d
                        ON li.image_id = d.image_id
                    left join public.dt_leg_ord_data_bwts bwts
                        on  bwts.ordnum =  split_part(d.order_no, '-', 1) and bwts.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT
                    left join public.dt_leg_ord_data_scrubber scrubber
                        on  scrubber.ordnum =  split_part(d.order_no, '-', 1) and scrubber.ordseq = NULLIF(split_part(d.order_no, '-', 2), '')::INT;
                    ''', (camera_id,))

                # 결과 가져오기
                input_rows = detection_cur.fetchall()
                input_col_names = [desc[0] for desc in detection_cur.description]

                # 데이터 변환
                input_data = [
                    {col: (row[idx] if row[idx] is not None else "NULL") for idx, col in enumerate(input_col_names)}
                    for row in input_rows
                ]

                input_data = [d for d in input_data if not d["object_label"].startswith("BWTS_")]
                print('input_data', input_data)

                if len(input_data) == 1 and input_data[0]['object_label'] == "NULL":
                    input_data = []

            all_data = filtered_results + detection_data + input_data

            return all_data

    except Exception as e:
        print("❌ DB 조회 중 오류:", e)
        return {"error": str(e)}

    finally:
        if conn:
            cur.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def seconds_to_hms(seconds):
    """
    초를 H:MM:SS 형식으로 변환합니다.
    
    Parameters:
        seconds (float): 변환할 초
        
    Returns:
        str: H:MM:SS 형식의 문자열
    """
    if seconds is None:
        return "0:00:00"
    
    # 초를 정수로 변환
    seconds = int(seconds)
    
    # 시, 분, 초 계산
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    
    # H:MM:SS 형식으로 반환
    return f"{hours}:{minutes:02d}:{seconds:02d}"

def get_detection_history(start_date, end_date, object_label=None, detected_row=None, detected_col=None):
    """
    tb_twin_detection_history 테이블에서 지정된 날짜 범위 내의 탐지 기록을 조회합니다.
    해당 기간의 평균 진행시간도 함께 계산하여 반환합니다.
    각 카메라 ID별 평균 진행시간과 행-열 쌍별 평균 진행시간도 계산하여 반환합니다.
    진행시간은 초, 분, H:MM:SS 형식으로 제공됩니다.
    특정 행(row)과 열(col)로 필터링할 수 있습니다.
    
    Parameters:
        start_date (str): 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str): 종료 날짜 (YYYY-MM-DD 형식)
        object_label (str, optional): 필터링할 객체 라벨 (기본값: None, 모든 객체 포함)
        detected_row (int, optional): 필터링할 행 번호 (기본값: None, 모든 행 포함)
        detected_col (int, optional): 필터링할 열 번호 (기본값: None, 모든 열 포함)
    
    Returns:
        dict: 다음 항목을 포함하는 딕셔너리
            - data: 쿼리 결과 데이터
            - average: 전체 평균 진행시간
            - camera_averages: 카메라별 평균 진행시간
            - row_col_averages: 행-열 쌍별 평균 진행시간
            - label_averages: 객체 라벨별 평균 진행시간
    """
    try:
        # 날짜 형식 검증 및 변환
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return {
                "status": "error",
                "message": "날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용하세요."
            }
        
        # 날짜 범위 검증 (최대 1개월)
        date_diff = (end_date_obj - start_date_obj).days
        if date_diff < 0:
            return {
                "status": "error",
                "message": "종료 날짜는 시작 날짜보다 이후여야 합니다."
            }

        
        # 데이터베이스 연결
        conn = get_connection()
        if not conn:
            return {
                "status": "error",
                "message": "데이터베이스 연결에 실패했습니다."
            }
        
        cursor = conn.cursor()
        
        # SQL 쿼리 구성 - 위치 변화 추적 쿼리로 대체
        query = """
        WITH position_changes AS (
            SELECT
                camera_id, detected_row, detected_col, object_label, created_at,
                LAG(object_label) OVER (PARTITION BY camera_id, detected_row, detected_col ORDER BY created_at) as prev_label,
                LAG(created_at) OVER (PARTITION BY camera_id, detected_row, detected_col ORDER BY created_at) as prev_time
            FROM tb_twin_detection_history
            WHERE created_at >= %s
              AND created_at <= %s
        )
        SELECT
            camera_id, detected_row, detected_col,
            COALESCE(prev_label, 'FIRST') as prev_label,
            object_label as current_label,
            prev_time as start_time,
            created_at as end_time,
            CASE
                WHEN prev_time IS NOT NULL
                THEN EXTRACT(EPOCH FROM (created_at - prev_time))
                ELSE 0
            END as duration_seconds,
            CASE
                WHEN prev_label IS NULL THEN 'FIRST_RECORD'
                WHEN prev_label = object_label THEN 'NO_CHANGE'
                ELSE 'CHANGED'
            END as change_type
        FROM position_changes
        WHERE prev_label IS NOT NULL
        """
        
        params = [start_date, end_date + ' 23:59:59']  # 종료일 포함
        
        # object_label 필터 추가
        if object_label:
            query += " WHERE current_label LIKE %s"
            params.append(f'%{object_label}%')
        
        query += """
        ORDER BY camera_id, detected_row, detected_col, created_at
        """
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # 결과 변환
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]
        
        # 각 레코드에 H:MM:SS 형식의 진행시간 추가
        for item in result:
            if 'duration_seconds' in item and item['duration_seconds'] is not None:
                item['duration_hms'] = seconds_to_hms(item['duration_seconds'])
            else:
                item['duration_hms'] = "0:00:00"
        
        # 전체 평균 진행시간 계산
        if result:
            total_seconds = sum(item['duration_seconds'] for item in result if item['duration_seconds'] is not None)
            count = len(result)
            
            avg_seconds = total_seconds / count if count > 0 else 0
            avg_minutes = avg_seconds / 60 if avg_seconds > 0 else 0
            
            avg_info = {
                "avg_duration_seconds": avg_seconds,
                "avg_duration_minutes": avg_minutes,
                "avg_duration_hms": seconds_to_hms(avg_seconds)
            }
        else:
            avg_info = {
                "avg_duration_seconds": 0,
                "avg_duration_minutes": 0,
                "avg_duration_hms": "0:00:00"
            }
        
        # 카메라별 평균 진행시간 계산
        camera_averages = {}
        # 행-열 쌍별 평균 진행시간 계산
        row_col_averages = {}
        # 객체 라벨별 평균 진행시간 계산
        label_averages = {}
        
        for item in result:
            camera_id = item['camera_id']
            detected_row = item['detected_row']
            detected_col = item['detected_col']
            current_label = item['current_label']
            
            # 카메라별 데이터 누적
            if camera_id not in camera_averages:
                camera_averages[camera_id] = {
                    "total_seconds": 0,
                    "count": 0
                }
            
            # 행-열 쌍별 데이터 누적
            row_col_key = f"{detected_row}_{detected_col}"
            if row_col_key not in row_col_averages:
                row_col_averages[row_col_key] = {
                    "detected_row": detected_row,
                    "detected_col": detected_col,
                    "total_seconds": 0,
                    "count": 0
                }
                
            # 객체 라벨별 데이터 누적
            if current_label not in label_averages:
                label_averages[current_label] = {
                    "total_seconds": 0,
                    "count": 0
                }
            
            if item['duration_seconds'] is not None:
                camera_averages[camera_id]["total_seconds"] += item['duration_seconds']
                row_col_averages[row_col_key]["total_seconds"] += item['duration_seconds']
                label_averages[current_label]["total_seconds"] += item['duration_seconds']
                
            camera_averages[camera_id]["count"] += 1
            row_col_averages[row_col_key]["count"] += 1
            label_averages[current_label]["count"] += 1
        
        # 카메라별 평균 계산
        for camera_id, data in camera_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            avg_minutes = avg_seconds / 60 if avg_seconds > 0 else 0
            
            camera_averages[camera_id] = {
                "avg_duration_seconds": avg_seconds,
                "avg_duration_minutes": avg_minutes,
                "avg_duration_hms": seconds_to_hms(avg_seconds),
                "count": data["count"]
            }
            
        # 행-열 쌍별 평균 계산
        for row_col_key, data in row_col_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            avg_minutes = avg_seconds / 60 if avg_seconds > 0 else 0
            
            row_col_averages[row_col_key] = {
                "detected_row": data["detected_row"],
                "detected_col": data["detected_col"],
                "avg_duration_seconds": avg_seconds,
                "avg_duration_minutes": avg_minutes,
                "avg_duration_hms": seconds_to_hms(avg_seconds),
                "count": data["count"]
            }
            
        # 객체 라벨별 평균 계산
        for label, data in label_averages.items():
            avg_seconds = data["total_seconds"] / data["count"] if data["count"] > 0 else 0
            avg_minutes = avg_seconds / 60 if avg_seconds > 0 else 0
            
            label_averages[label] = {
                "avg_duration_seconds": avg_seconds,
                "avg_duration_minutes": avg_minutes,
                "avg_duration_hms": seconds_to_hms(avg_seconds),
                "count": data["count"]
            }
        
        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")
        
        return {
            "status": "success",
            "data": result,
            "average": avg_info,
            "camera_averages": camera_averages,
            "row_col_averages": row_col_averages,
            "label_averages": label_averages
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"오류가 발생했습니다: {str(e)}"
        }
    
    finally:
        if conn:
            cursor.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


def get_processing_time(start_date, end_date):
    """
    공정진행별 평균 진행 시간을 반환

    Query Parameters:
        start_date (str, required): 필수 - 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str, required): 필수 - 종료 날짜 (YYYY-MM-DD 형식)

    Returns:
        JSON: 
            - data: 쿼리 결과 데이터(bwts대상, 0%, 10%, 30%, 70%, 90%, 100% 각 단계의 평균 시간)
    """
    try:
        # 날짜 형식 검증 및 변환
        try:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return {
                "status": "error",
                "message": "날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용하세요."
            }
        
        # 데이터베이스 연결
        conn = get_connection()
        if not conn:
            return {
                "status": "error",
                "message": "데이터베이스 연결에 실패했습니다."
            }
        
        cursor = conn.cursor()

        # SQL 쿼리 구성 - 위치 변화 추적 쿼리로 대체
        # query = """
        # WITH base AS (
        #         SELECT
        #             camera_id,
        #             detected_row,
        #             detected_col,
        #             object_label,
        #             created_at,
        #             ROW_NUMBER() OVER (
        #                 PARTITION BY camera_id, detected_row, detected_col
        #                 ORDER BY created_at
        #             ) AS rn
        #         FROM tb_twin_detection_history
        #         WHERE object_label IN ('BWTS_0%%', 'BWTS_10%%', 'BWTS_30%%', 'BWTS_70%%', 'BWTS_90%%', 'BWTS_100%%',
        #         						'SCRUBBER_0%%', 'SCRUBBER_10%%', 'SCRUBBER_30%%', 'SCRUBBER_70%%', 'SCRUBBER_90%%', 
        #                                 'SCRUBBER_100%%')
        #                 and created_at between %s and %s
        #     ),
        #     joined AS (
        #         SELECT
        #             b2.object_label AS current_label,
        #             EXTRACT(EPOCH FROM (b2.created_at - b1.created_at)) / 60 AS duration_minutes,
        #             EXTRACT(EPOCH FROM (b2.created_at - b1.created_at)) / 60 / 24 AS duration_day
        #         FROM base b1
        #         JOIN base b2
        #         ON b1.camera_id = b2.camera_id
        #         AND b1.detected_row = b2.detected_row
        #         AND b1.detected_col = b2.detected_col
        #         AND b2.rn = b1.rn + 1  -- b2가 이후 상태
        #     WHERE b1.created_at IS NOT NULL AND b2.created_at IS NOT NULL
        #     )
        #     SELECT
        #         current_label,
        #         AVG(duration_minutes) AS avg_time_to_reach_minutes,
        #         AVG(duration_day) AS avg_time_to_reach_day
        #     FROM joined
        #     GROUP BY current_label
        #     ORDER BY current_label;
        # """

        query = """
        WITH base AS (
                SELECT
                    camera_id,
                    detected_row,
                    detected_col,
                    object_label,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY camera_id, detected_row, detected_col
                        ORDER BY created_at
                    ) AS rn
                FROM tb_twin_detection_history
                WHERE object_label IN ('BWTS_0%%', 'BWTS_10%%', 'BWTS_30%%', 'BWTS_70%%', 'BWTS_90%%', 'BWTS_100%%',
                						'SCRUBBER_90%%', 
                                        'SCRUBBER_100%%')
                        and created_at between %s and %s
            ),
            joined AS (
                SELECT
                    b2.object_label AS current_label,
                    EXTRACT(EPOCH FROM (b2.created_at - b1.created_at)) / 60 AS duration_minutes,
                    EXTRACT(EPOCH FROM (b2.created_at - b1.created_at)) / 60 / 24 AS duration_day
                FROM base b1
                JOIN base b2
                ON b1.camera_id = b2.camera_id
                AND b1.detected_row = b2.detected_row
                AND b1.detected_col = b2.detected_col
                AND b2.rn = b1.rn + 1  -- b2가 이후 상태
            WHERE b1.created_at IS NOT NULL AND b2.created_at IS NOT NULL
            )
            SELECT
                current_label,
                AVG(duration_minutes) AS avg_time_to_reach_minutes,
                AVG(duration_day) AS avg_time_to_reach_day
            FROM joined
            GROUP BY current_label
            ORDER BY current_label;
        """

        params = [start_date + ' 00:00:00', end_date + ' 23:59:59']  # 종료일 포함

        cursor.execute(query, params)
        rows = cursor.fetchall()

        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()
        print("Closed to PostgreSQL successfully.")

        return {
            "status": "success",
            "data": result,
        }

        
    except Exception as e:
        return {
            "status": "error",
            "message": f"오류가 발생했습니다: {str(e)}"
        }
    
    finally:
        if conn:
            cursor.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")

def create_directory_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Directory '{path}' has been created.")
    else:
        print(f"Directory '{path}' already exists.")

def get_camera_history(start_date, end_date, comp_id, event_type):
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            date_trunc('second', ceh.event_time) AS event_time,
            ceh.event_type,
            ceh.event_desc,

            json_agg(
                json_build_object(
                    'camera_id', tc.camera_id,
                    'file_path', ceh.file_path
                )
                ORDER BY tc.camera_id, ceh.file_path
            ) AS images,
            bool_or(ceh.isread) AS isread,
            max(ceh.remark) AS remark
        FROM public.tb_camera_event_hist ceh
        LEFT JOIN public.tb_camera tc
            ON ceh.camera_id = tc.camera_id
        WHERE ceh.event_time BETWEEN %s AND %s
    """

    params = [
        start_date + " 00:00:00",
        end_date + " 23:59:59",
    ]

    if comp_id is not None:
        query += " AND tc.comp_id = %s "
        params.append(comp_id)

    if event_type:
        query += " AND ceh.event_type = ANY(%s) "
        params.append(event_type)

    query += """
        GROUP BY
            date_trunc('second', ceh.event_time),
            ceh.event_type,
            ceh.event_desc
        ORDER BY event_time DESC
    """

    cursor.execute(query, tuple(params))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows