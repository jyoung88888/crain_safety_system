import sys
import os
import requests
from lib.public_func import get_connection


# ────────── CCTV별 모니터링 그룹 조회 ──────────
def get_monitoring_groups_by_cctv_id(cctv_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT monitoring_grp_id FROM tb_camera_monitoring_layout WHERE camera_id = %s", (cctv_id,))
        result = cursor.fetchall()
        return [row[0] for row in result] if result else []
    except Exception as e:
        print(f"Error fetching monitoring_grp_id for comp_id {cctv_id}: {e}", file=sys.stderr, flush=True)
        return []
    finally:
        cursor.close()
        conn.close()


# ────────── 텔레그램 알림 수신자 조회 ──────────
def get_chat_id_with_notification(monitoring_group_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
                       SELECT chat_id, token
                       FROM tb_telegram_managers
                       WHERE monitoring_grp_id = %s
                         AND notification_on = true
                       """, (monitoring_group_id,))
        result = cursor.fetchone()
        return (result[0], result[1]) if result else None
    except Exception as e:
        print(f"Error fetching chat_id: {e}", file=sys.stderr, flush=True)
        return None
    finally:
        cursor.close()
        conn.close()


# ────────── 텔레그램 알림 전송 ──────────
def send_telegram_alert(cctv_id, class_name, file_path=None):
    event_mapping = {
        "Emergency": "E001",
    }
    if class_name not in event_mapping:
        print(f"Unknown class_name: {class_name}", file=sys.stderr, flush=True)
        return

    event_type = event_mapping[class_name]
    monitoring_grp_ids = get_monitoring_groups_by_cctv_id(cctv_id)
    if not monitoring_grp_ids:
        return

    chat_ids = []
    for mg_id in monitoring_grp_ids:
        cfg = get_chat_id_with_notification(mg_id)
        if not cfg:
            continue
        chat_id, token = cfg
        chat_ids.append({'chat_id': chat_id, 'token': token})

    if not chat_ids:
        return

    messages = {
        "E001": f"[경고]\n(CCTV ID: {cctv_id})\n위험 상황 감지!",
    }
    message = messages.get(event_type, "[알림] 이벤트가 감지되었습니다.")

    if file_path and os.path.exists(file_path):
        file_ext = os.path.splitext(file_path)[-1].lower()
        for item in chat_ids:
            if file_ext in ['.jpg', '.png', '.jpeg']:
                url_file = f"https://api.telegram.org/bot{item['token']}/sendPhoto"
                file_key = 'photo'
            elif file_ext in ['.mp4', '.avi', '.mov']:
                url_file = f"https://api.telegram.org/bot{item['token']}/sendVideo"
                file_key = 'video'
            else:
                return
            try:
                with open(file_path, 'rb') as f:
                    files = {file_key: f}
                    data_file = {'chat_id': item['chat_id'], 'caption': message}
                    response = requests.post(url_file, data=data_file, files=files, timeout=10)
                if response.status_code == 200:
                    print(f"Telegram file sent (chat_id: {item['chat_id']})", flush=True)
                else:
                    print(f"Telegram file failed (chat_id: {item['chat_id']}): {response.status_code}", file=sys.stderr, flush=True)
            except requests.RequestException as e:
                print(f"Telegram exception (chat_id: {item['chat_id']}): {e}", file=sys.stderr, flush=True)
    else:
        for item in chat_ids:
            try:
                url_message = f"https://api.telegram.org/bot{item['token']}/sendMessage"
                data_message = {'chat_id': item['chat_id'], 'text': message}
                response = requests.post(url_message, data=data_message, timeout=10)
            except requests.RequestException as e:
                print(f"Telegram exception (chat_id: {item['chat_id']}): {e}", file=sys.stderr, flush=True)
