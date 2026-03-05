from flask import Blueprint, request, jsonify
import requests
import json
import os
from blueprints.lib.public_func import get_all_cameras, get_ai_server, update_camera_pid, get_camera_by_id
from lib.db import get_connection
import psutil

# 서버 실행 관련 api
# 블루프린트 생성, /cctv/remote 상위 경로 추가
cctv_remote = Blueprint('cctv_remote', __name__, url_prefix='/cctv/remote')

@cctv_remote.route('/stop_all', methods=['GET'])
def stop_all():
    # CCTV_DATA 전체 로드
    cctv_data = get_all_cameras()
    # print(cctv_data)
    for cctv in cctv_data:
        # print(cctv)
        if cctv[7] != '':
            server_id = cctv[4]
            server_ip = get_ai_server(server_id)[3]
            restapi_port = get_ai_server(server_id)[4]
            # print(server_ip)
            # POST 요청을 보낼 URL
            url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/terminate_process'
            # print(url)
            # 요청에 포함할 데이터 (JSON 형식)
            data = {
                "pid": int(cctv[7]),
                "cctv_id": cctv[1]
            }
            # print(data)
            # 헤더 (옵션, 필요 시 설정)
            headers = {
                'Content-Type': 'application/json'
            }
            # POST 요청 보내기
            try:
                response = requests.post(url, data=json.dumps(data), headers=headers)
                # 응답 처리
                if response.status_code == 200:
                    print("POST 요청 성공")
                    print("응답 데이터:", response.json())
                    # pid 상태 저장
                    # cctv_data[cctv]['pid'] = response.json()['process_pid']
                    update_camera_pid(cctv[1], '', None, False)
                else:
                    print("POST 요청 실패")
                    print("상태 코드:", response.status_code)
                    print("응답 내용:", response.text)
            except Exception as e:
                print(e)

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": f"run successfully"})

# cctv 전체 실행
@cctv_remote.route('/run_all', methods=['GET'])
def run_all():
    # CCTV_DATA 전체 로드
    cctv_data = get_all_cameras()
    # print(cctv_data)
    for cctv in cctv_data:
        # print(cctv)
        if cctv[7] == '':
            server_id = cctv[4]
            server_ip = get_ai_server(server_id)[3]
            restapi_port = get_ai_server(server_id)[4]
            # print(server_ip)
            # POST 요청을 보낼 URL
            url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/run_ai_cctv'
            # print(url)
            # 요청에 포함할 데이터 (JSON 형식)
            data = {
                "in_url": cctv[5],
                "out_path": cctv[6],
                "cctv_id": cctv[1],
                "jit_only": cctv[13],
            }
            # print(data)
            # 헤더 (옵션, 필요 시 설정)
            headers = {
                'Content-Type': 'application/json'
            }
            # POST 요청 보내기
            try:
                # response = requests.post(url, data=json.dumps(data), headers=headers)
                response = requests.post(url, json=data, timeout=10)
                # 응답 처리
                if response.status_code == 200:
                    print("POST 요청 성공")
                    print("응답 데이터:", response.json())
                    # 배치 모드에서 buffered 상태면 DB 업데이트 스킵
                    if not response.json().get('buffered'):
                        update_camera_pid(cctv[1], response.json()['process_pid'], response.json()['port'], True)
                else:
                    print("POST 요청 실패")
                    print("상태 코드:", response.status_code)
                    print("응답 내용:", response.text)
            except Exception as e:
                print(e)

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": f"run successfully"})


# cctv_id 재생용 url 받기
@cctv_remote.route('/get_cctv_play_url/<string:cctv_id>', methods=['GET'])
def get_cctv_play_url(cctv_id):

    # cctv_id에 해당되는 CCTV_DATA 로드
    cctv_data = get_camera_by_id(cctv_id)

    server_id = cctv_data[4]
    # server_ip = get_ai_server(server_id)[3]

    server_url = get_ai_server(server_id)[11]

    mediamtx_port = get_ai_server(server_id)[5]
    # POST 요청을 보낼 URL
    url = f"{server_url}/{cctv_data[6]}/?controls=0"
    print(url, flush=True)
    # url = server_url + '/stream/' + cctv_data[6] + '/'

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "play url": url})


#pid 검증
def is_pid_alive(pid_str) -> bool:
    try:
        pid = int(pid_str)
        return pid > 1 and psutil.pid_exists(pid)
    except:
        return False
# CCTV ID 별 실행
def start_cctv_by_id(cctv_id: str, force: bool = False) -> dict:
    import time as _time
    _t0 = _time.time()
    print(f"[DEBUG] start_cctv_by_id({cctv_id}) START", flush=True)
    cctv_data = get_camera_by_id(cctv_id)
    if not cctv_data:
        return {"success": False, "code": 404, "msg": "camera not found"}

    pid = cctv_data[7]

    # pid가 있으면 "진짜 살아있는지" 확인
    if pid and str(pid).strip() != "":
        if not force and is_pid_alive(pid):
            return {"success": True, "code": 200, "msg": "already running", "skipping": True}

        # pid는 있는데 실제 프로세스는 없음(재시작/크래시) → pid 정리
        update_camera_pid(cctv_data[1], "", None)

    server_id = cctv_data[4]
    ai_server = get_ai_server(server_id)
    server_ip = ai_server[3]
    restapi_port = ai_server[4]
    jit_only = cctv_data[13]

    url = f'http://{server_ip}:{restapi_port}/cctv/process/run_ai_cctv'
    data = {
        "in_url" : cctv_data[5],
        "out_path" : cctv_data[6],
        "cctv_id" : cctv_id,
        "jit_only" : jit_only
    }
    print(f"[DEBUG] {cctv_id} POST -> {url}", flush=True)
    print(f"[DEBUG] {cctv_id} jit_only={jit_only} (type={type(jit_only).__name__})", flush=True)
    try:
        resp = requests.post(url, data=json.dumps(data), headers={'Content-Type': 'application/json'}, timeout=10)
        _elapsed = _time.time() - _t0
        print(f"[DEBUG] {cctv_id} response status={resp.status_code}, elapsed={_elapsed:.3f}s", flush=True)
        print(f"[DEBUG] {cctv_id} response body={resp.text}", flush=True)
        if resp.status_code != 200:
            return {"success": False,
                    "code": resp.status_code,
                    "msg": resp.text}

        body = resp.json()
        # 배치 모드에서 buffered 상태면 DB 업데이트 스킵 (_launch_batch에서 일괄 처리)
        if not body.get('buffered'):
            update_camera_pid(cctv_data[1], body.get('process_pid'), body.get('port'), True)

        return {"success": True,
                "code": 200,
                "msg": "run successfully",
                "process_pid": body.get('process_pid'),
                "port": body.get('port'),
                "buffered": body.get('buffered', False)}
    except Exception as e:
        _elapsed = _time.time() - _t0
        print(f"[DEBUG] {cctv_id} EXCEPTION after {_elapsed:.3f}s: {e}", flush=True)
        return {"success": False,
                "code": 500,
                "msg": str(e)}


# cctv_id 실행
@cctv_remote.route('/run_cctv/<string:cctv_id>', methods=['GET'])
def run_cctv(cctv_id):

    result = start_cctv_by_id(cctv_id)
    return jsonify(result), result.get("code", 500)

# cctv_id 종료
@cctv_remote.route('/stop_cctv/<string:cctv_id>', methods=['GET'])
def stop_cctv(cctv_id):

    # cctv_id에 해당되는 CCTV_DATA 로드
    cctv_data = get_camera_by_id(cctv_id)

    if cctv_data == None:
        # print("None")
        return jsonify({"message": f"run fail"})

    if cctv_data[7] != '':
        server_id = cctv_data[4]
        server_ip = get_ai_server(server_id)[3]
        restapi_port = get_ai_server(server_id)[4]
        # POST 요청을 보낼 URL
        url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/terminate_process'
        # print(url)
        # 요청에 포함할 데이터 (JSON 형식)
        data = {
            "pid": int(cctv_data[7]),
            "cctv_id": cctv_id
        }

        # 헤더 (옵션, 필요 시 설정)
        headers = {
            'Content-Type': 'application/json'
        }

        print(data)

        # POST 요청 보내기
        try:
            # response = requests.post(url, data=json.dumps(data), headers=headers)
            response = requests.post(url, json=data, timeout=10)
            # 응답 처리
            if response.status_code == 200:
                print("POST 요청 성공")
                print("응답 데이터:", response.json())
                # pid 상태 저장
                update_camera_pid(cctv_data[1], "", None, False)
                return jsonify({
                    "success": True,
                    "code": 200,
                    "msg": "성공하였습니다.",
                    "message": f"stop successfully"})
            else:
                print("POST 요청 실패")
                print("상태 코드:", response.status_code)
                print("응답 내용:", response.text)
        except Exception as e:
            print(e)

    return jsonify({"success": False,
                    "code": 404,
                    "msg": f"stop fail"})


# 전체 CCTV PID 확인
@cctv_remote.route('/cctv_pid_chk/', methods=['GET'])
def cctv_pid_chk():
    

    # CCTV_DATA 전체 로드
    cctv_data = get_all_cameras()
    for cctv in cctv_data:
        if cctv[7] != '':
            # pid 실행여부 확인
            server_id = cctv[4]
            server_ip = get_ai_server(server_id)[3]
            restapi_port = get_ai_server(server_id)[4]
            # print(server_ip)
            # POST 요청을 보낼 URL
            url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/chk_pid'
            # print(url)
            # 요청에 포함할 데이터 (JSON 형식)
            data = {
                "pid": cctv[7],
            }
            # print(data)
            # 헤더 (옵션, 필요 시 설정)
            headers = {
                'Content-Type': 'application/json'
            }
            # POST 요청 보내기
            try:
                response = requests.post(url, data=json.dumps(data), headers=headers)

                # 응답 처리
                # 존재가 없으면 pid 삭제
                if not response.json()['pid exists']:
                    update_camera_pid(cctv[1], '', None, False)
            except Exception as e:
                print(e)
                # 서버연결 오류시 pid 삭제
                update_camera_pid(cctv[1], '', None, False)

    # save_data(cctv_data, CCTV_DATA_FILE)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": f"chk successfully"})

def bootstrap_run_enabled_cctv():
    if os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    
    rows = get_cameras_run_enabled()
    print("ROWS " + str(rows), flush=True)
    for row in rows:
        camera_id = row[0]
        print("camera_id : " + camera_id, flush=True)
        r = start_cctv_by_id(camera_id, force=True)
        print(r, flush=True)
        print(f"[BOOTSTRAP] {camera_id} => {r}", flush=True)

def get_cameras_run_enabled():
    ctx = "[BOOTSTRAP] get_cameras_run_enabled"
    conn = get_connection(ctx)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT camera_id, pid
            FROM tb_camera
            WHERE run_yn = true
        """)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
        print(f"[DB Closed] {ctx}")