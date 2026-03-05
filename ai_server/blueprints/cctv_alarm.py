from flask import Blueprint, request, jsonify
import os
from config.config import Config
import requests
# from blueprints.lib.public_func import

# 알람 정보
# 블루프린트 생성, /cctv/cctv_alarm 상위 경로 추가
cctv_alarm = Blueprint('cctv_alarm', __name__, url_prefix='/cctv/cctv_alarm')

cctv_ip = os.getenv("CCTV_IP", Config.CCTV_IP)
CCTV_BASE_URL = f"http://{cctv_ip}"

@cctv_alarm.route('/on', methods=['POST'])
def alarm_on():
    body = request.get_json(silent=True) or {}
    relay = int(body.get("relay", 1))

    try:
        r = requests.get(
            f"{CCTV_BASE_URL}/update",
            params={"relay": relay, "state": 1},
            timeout=2.0
        )
        r.raise_for_status()
        return jsonify({"success": True, "code": 200, "msg": "alarm on", "device": r.url})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "code": 504, "msg": "device timeout"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "code": 502, "msg": "device error", "error": str(e)}), 502


@cctv_alarm.route('/off', methods=['POST'])
def alarm_off():
    body = request.get_json(silent=True) or {}
    relay = int(body.get("relay", 1))

    try:
        r = requests.get(
            f"{CCTV_BASE_URL}/update",
            params={"relay": relay, "state": 0},
            timeout=2.0
        )
        r.raise_for_status()
        return jsonify({"success": True, "code": 200, "msg": "alarm off", "device": r.url})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "code": 504, "msg": "device timeout"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "code": 502, "msg": "device error", "error": str(e)}), 502
    
def set_alarm(state: int, relay: int = 1, timeout: float = 2.0):
    """
    state: 1(on), 0(off)
    return: dict (요청 URL/응답)
    """
    try:
        r = requests.get(
            f"{CCTV_BASE_URL}/update",
            params={"relay": int(relay), "state": int(state)},
            timeout=timeout
        )
        r.raise_for_status()
        return {
            "request_url": r.url,
            "status_code": r.status_code,
            "response_text": r.text
        }
    except requests.exceptions.Timeout:
        print(f"[ALARM] 장비 타임아웃 (relay={relay}, state={state})", flush=True)
        return {"error": "timeout"}
    except requests.exceptions.RequestException as e:
        print(f"[ALARM] 장비 통신 실패: {e}", flush=True)
        return {"error": str(e)}
    
@cctv_alarm.route('/control', methods=['POST'])
def alarm_control():
    body = request.get_json(silent=True) or {}

    # 1) 입력값 파싱/기본값
    state = body.get("state")  # 필수 (0/1)
    relay = body.get("relay", 1)
    timeout = body.get("timeout", 2.0)

    if state is None:
        return jsonify({"success": False, "code": 400, "msg": "state is required (0 or 1)"}), 400
    if state not in (0, 1):
        return jsonify({"success": False, "code": 400, "msg": "state must be 0(off) or 1(on)"}), 400

    
    result = set_alarm(state=state, relay=relay, timeout=timeout)

    # 4) 결과 리턴
    if "error" in result:
        # timeout / 통신 실패 구분하고 싶으면 error 값으로 분기 가능
        return jsonify({
            "success": False,
            "code": 502,
            "msg": "alarm control failed"
        }), 502

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "alarm on" if state == 1 else "alarm off"
    }), 200