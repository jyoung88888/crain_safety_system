import base64
import math
import zmq
import numpy as np
import io
from PIL import Image
import os
import json
import socket
from flask import Blueprint, request, jsonify, send_from_directory, Response
import subprocess
import threading
from blueprints.lib.public_func import process_dict, port_dict, find_largest_jpg_file, get_image_size, \
    get_imgfile_size, terminate_process_tree, is_port_in_use, update_camera_pid
from config.config import Config

cctv_process = Blueprint('cctv_process', __name__, url_prefix='/cctv/process')

_port_lock = threading.Lock()

# ── 멀티카메라 배치 실행 ──
_batch_lock = threading.Lock()
_batch_buffer = {}           # {cctv_id: {in_url, out_path, port}}
_batch_timer = None          # threading.Timer
_batch_ai_process = None     # rtsp_ai_one_zone.py subprocess
_batch_sm_process = None     # state_manager.py subprocess
_batch_ai_log_fh = None      # rtsp_ai_one_zone.py log file handle
_batch_sm_log_fh = None      # state_manager.py log file handle
_batch_cam_ids = []          # 실행된 배치의 카메라 ID 리스트
_batch_port_map = {}         # {out_path: port} (이미지 소켓용)
BATCH_CAMERA_COUNT = 3       # 최대 배치 카메라 수
BATCH_WAIT_SEC = 10          # 첫 카메라 도착 후 대기 시간 (초)

def is_port_in_use(port: int, host="0.0.0.0") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False  # bind 성공 = 안 쓰는 포트
        except OSError:
            return True   # bind 실패 = 사용중

def _launch_batch():
    """버퍼에 있는 카메라들로 rtsp_ai_one_zone.py + state_manager.py 실행"""
    global _batch_ai_process, _batch_sm_process, _batch_cam_ids, _batch_timer
    global _batch_ai_log_fh, _batch_sm_log_fh

    with _batch_lock:
        if not _batch_buffer:
            return None
        cams = dict(_batch_buffer)
        _batch_buffer.clear()
        _batch_timer = None

    # 기존 배치 프로세스가 있으면 먼저 종료
    if _batch_ai_process and _batch_ai_process.poll() is None:
        print(f"[BATCH] 기존 AI 프로세스 종료 (PID={_batch_ai_process.pid})", flush=True)
        terminate_process_tree(_batch_ai_process.pid)
        process_dict.pop(_batch_ai_process.pid, None)
    if _batch_sm_process and _batch_sm_process.poll() is None:
        print(f"[BATCH] 기존 StateManager 종료 (PID={_batch_sm_process.pid})", flush=True)
        terminate_process_tree(_batch_sm_process.pid)
    for fh in (_batch_ai_log_fh, _batch_sm_log_fh):
        if fh:
            try:
                fh.close()
            except Exception:
                pass
    for cid in _batch_cam_ids:
        update_camera_pid(cid, '', None, False)
    _batch_port_map.clear()

    cam_ids = list(cams.keys())
    cam_list = list(cams.values())

    print(f"[BATCH] LAUNCH ({len(cam_ids)} cameras: {cam_ids})", flush=True)

    import sys
    python_cmd = sys.executable
    base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    log_dir = os.path.join(base_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # rtsp_ai_one_zone.py 실행
    ai_log_file = os.path.join(log_dir, 'batch_ai.log')
    ai_script = os.path.join(base_dir, 'rtsp_service', 'rtsp_ai_one_zone.py')
    command = [
        python_cmd, '-u',
        ai_script,
        '--cctv_ids',  ','.join(cam_ids),
        '--in_urls',   ','.join(c['in_url'] for c in cam_list),
        '--out_paths', ','.join(c['out_path'] for c in cam_list),
        '--ports',     ','.join(str(c['port']) for c in cam_list),
        '--rtmp_host', Config.RTMP_HOST,
        '--rtmp_port', str(Config.RTMP_PORT),
    ]
    ai_log_fh = open(ai_log_file, 'a', encoding='utf-8')
    ai_proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=ai_log_fh, cwd=base_dir)

    # state_manager.py 실행
    sm_log_file = os.path.join(log_dir, 'state_manager.log')
    sm_script = os.path.join(base_dir, 'rtsp_service', 'state_manager.py')
    sm_command = [python_cmd, '-u', sm_script]
    sm_log_fh = open(sm_log_file, 'a', encoding='utf-8')
    sm_proc = subprocess.Popen(sm_command, stdout=subprocess.DEVNULL, stderr=sm_log_fh, cwd=base_dir)

    print(f"[BATCH] AI PID={ai_proc.pid}, StateManager PID={sm_proc.pid}", flush=True)

    # 글로벌 상태 갱신
    with _port_lock:
        for cid in cam_ids:
            port_dict.pop(f"_pending_{cid}", None)
        process_dict[ai_proc.pid] = ai_proc

    _batch_ai_process = ai_proc
    _batch_sm_process = sm_proc
    _batch_ai_log_fh = ai_log_fh
    _batch_sm_log_fh = sm_log_fh
    _batch_cam_ids = list(cam_ids)

    for cid, cam in cams.items():
        _batch_port_map[cam['out_path']] = cam['port']

    # 모든 카메라 DB PID 업데이트
    for cid, cam in cams.items():
        update_camera_pid(cid, ai_proc.pid, cam['port'], True)

    return ai_proc.pid


@cctv_process.route('/run_ai_cctv', methods=['POST'])
def run_ai_cctv():
    global _batch_timer

    data = request.json
    in_url = data.get('in_url')
    out_path = data.get('out_path')
    cctv_id = data.get('cctv_id')
    jit_only = data.get('jit_only')
    print(f"[run_ai_cctv] cctv_id={cctv_id}, jit_only={jit_only}", flush=True)

    # jit_only 처리 (기존과 동일)
    if jit_only == True:
        return jsonify({
            'in_url': in_url, 'out_path': out_path,
            'process_pid': 1, 'port': None,
            'status': 'success', 'success': True, 'code': 200,
            'msg': '성공하였습니다.',
        })

    # 포트 할당
    use_port = None
    with _port_lock:
        assigned_ports = set(port_dict.values()) | set(_batch_port_map.values())
        print(f"[PORT] {cctv_id} - assigned={assigned_ports}", flush=True)
        for port in range(9003, 9999):
            if port not in assigned_ports and not is_port_in_use(port):
                use_port = port
                break
        if use_port is None:
            return jsonify({"success": False, "code": 500, "msg": "no available port"})
        reserve_key = f"_pending_{cctv_id}"
        port_dict[reserve_key] = use_port
        print(f"[PORT] {cctv_id} => {use_port} (reserved)", flush=True)

    # 배치 버퍼에 추가
    should_launch = False
    with _batch_lock:
        _batch_buffer[cctv_id] = {
            'in_url': in_url,
            'out_path': out_path,
            'port': use_port,
        }
        buf_count = len(_batch_buffer)
        print(f"[BATCH] {cctv_id} buffered ({buf_count}/{BATCH_CAMERA_COUNT})", flush=True)

        if buf_count >= BATCH_CAMERA_COUNT:
            # N대 도달 → 즉시 실행
            if _batch_timer:
                _batch_timer.cancel()
                _batch_timer = None
            should_launch = True
        elif buf_count == 1:
            # 첫 카메라 → 타이머 시작
            _batch_timer = threading.Timer(BATCH_WAIT_SEC, _launch_batch)
            _batch_timer.daemon = True
            _batch_timer.start()
            print(f"[BATCH] Timer started ({BATCH_WAIT_SEC}s)", flush=True)

    if should_launch:
        pid = _launch_batch()
        return jsonify({
            'in_url': in_url, 'out_path': out_path,
            'process_pid': pid or 0, 'port': use_port,
            'status': 'success', 'success': True, 'code': 200,
            'msg': '성공하였습니다.', 'buffered': False,
        })
    else:
        return jsonify({
            'in_url': in_url, 'out_path': out_path,
            'process_pid': 0, 'port': use_port,
            'status': 'success', 'success': True, 'code': 200,
            'msg': f'배치 대기 중 ({buf_count}/{BATCH_CAMERA_COUNT})',
            'buffered': True,
        })

@cctv_process.route('/get_img/<string:out_path>', methods=['GET'])
def get_img(out_path):
    print("out_path:", out_path, flush=True)

    folder = f'./img/{out_path}_short'

    # 1) 폴더 존재 확인
    if not os.path.isdir(folder):
        return jsonify({
            "success": False,
            "code": 404,
            "msg": f"이미지 폴더가 없습니다: {folder}",
        }), 404

    # 2) 가장 큰 jpg 찾기
    img_nm = find_largest_jpg_file(folder)
    print("img_nm:", img_nm, flush=True)

    if not img_nm:
        return jsonify({
            "success": False,
            "code": 404,
            "msg": f"이미지 파일(jpg)을 찾을 수 없습니다: {folder}",
        }), 404

    img_path = os.path.join(folder, img_nm)

    # 3) 이미지 크기 (실패시 기본값)                                                             
    try:
        width, height = get_image_size(img_path)
    except Exception as e:
        print(f"get_image_size Error: {e}")
        width, height = 1920, 1080

    # 4) 파일 읽기 (없으면 404)
    try:
        with open(img_path, 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    except FileNotFoundError:
        return jsonify({
            "success": False, 
            "code": 404,
            "msg": f"이미지 파일이 없습니다: {img_path}",
        }), 404

    gcd = math.gcd(width, height)
    width_ratio = width // gcd
    height_ratio = height // gcd

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "img_decode_data": encoded_image,
        "img_size": {"width": width, "height": height},
        "ratio": f"{width_ratio}:{height_ratio}",
    })

# cctv 이미지 받기
@cctv_process.route('/get_img_socket', methods=['POST'])
def get_img_socket():
    cctv_data = request.json
    print(cctv_data, flush=True)
    pid = cctv_data['pid']
    out_path = cctv_data['out_path']
    folder = f'./img/{out_path}_short'


    encoded_image = None
    width = None
    height = None
    #소켓통신으로 타 프로스세
    # 에서 이미지 받기
    if pid != '':
        if pid != '1':
            # 배치 모드: out_path로 포트 조회, 폴백으로 기존 port_dict
            port = _batch_port_map.get(out_path) or port_dict.get(int(pid))
            if port:
                context = zmq.Context()
                socket = context.socket(zmq.REQ)
                socket.connect("tcp://localhost:" + str(port))
                print(f"tcp://localhost:{port}", flush=True)
                print(f"pid={pid} port={port} out_path={out_path}", flush=True)                

                # 응답 타임아웃 설정 (5000ms = 5초)
                socket.setsockopt(zmq.RCVTIMEO, 5000)
                
                
                try:
                    socket.send_string(json.dumps({
                        "cmd": "send_image",
                        "cctv_id": out_path,   # 또는 pid, 또는 CAM0005 같은 id
                    }, ensure_ascii=False))
                    response = socket.recv_string()
                    print(f"Received data length: {len(response)}", flush=True)
                    
                    if response.startswith("Error"):
                        print(f"Server error: {response}")
                    else:
                        # Base64 문자열을 디코딩하여 이미지 복원
                        
                        image_data = base64.b64decode(response)
                        image_array = np.frombuffer(image_data, dtype=np.uint8)
                        # encoded_image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
                        image = Image.open(io.BytesIO(image_array))
                        width, height = image.size
                        # image: PIL.Image 객체
                        buffer = io.BytesIO()
                        image.save(buffer, format="JPEG")  # 원하는 이미지 포맷으로 저장
                        buffer.seek(0)  # 스트림의 시작 위치로 이동
                        # 바이트 스트림을 Base64로 인코딩
                        encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                        
                except zmq.Again:
                    # 타임아웃이 발생한 경우 처리
                    print("Error: No response from server (timeout). Exiting...")
                    # create_directory_if_not_exists(folder)
                    img_nm = find_largest_jpg_file('./img/' + out_path +'_short')
                    print(img_nm)
                    width, height = get_image_size('./img/' + out_path +'_short/' + img_nm)
                    # print(width, height)

                    #이미지 파일 로드
                    with open('./img/' + out_path +'_short/' + img_nm, 'rb') as image_file:
                        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
                # except Exception as e:
                #     print(f"ZMQ error: {type(e).__name__}: {e}", flush=True)
                #     # fallback...
                #     img_nm = find_largest_jpg_file(f'./img/{out_path}_short')
                #     width, height = get_image_size(f'./img/{out_path}_short/{img_nm}')
                #     with open(f'./img/{out_path}_short/{img_nm}', 'rb') as image_file:
                #         encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

                finally:
                    # ★ 이게 없으면 점점 망가짐
                    socket.close(0)
                    context.term()

        else:
            port = 9001
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect("tcp://localhost:" + str(port))
            print("9001 tcp://localhost:" +  str(port), flush=True)

            # 응답 타임아웃 설정 (5000ms = 5초)
            socket.setsockopt(zmq.RCVTIMEO, 5000)
            
            try:
                # 서버에 이미지 요청
                socket.send_string(cctv_data['cctv_id'])
                
                # 서버로부터 응답 수신
                response = socket.recv_string()
                print(f"Received data length 9001: {len(response)}", flush=True)
                
                if response.startswith("Error"):
                    print(f"Server error: {response}")
                else:
                    # Base64 문자열을 디코딩하여 이미지 복원
                    image_data = base64.b64decode(response)
                    image_array = np.frombuffer(image_data, dtype=np.uint8)
                    # encoded_image = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
                    image = Image.open(io.BytesIO(image_array))
                    width, height = image.size
                    # image: PIL.Image 객체
                    buffer = io.BytesIO()
                    image.save(buffer, format="JPEG")  # 원하는 이미지 포맷으로 저장
                    buffer.seek(0)  # 스트림의 시작 위치로 이동

                    # 바이트 스트림을 Base64로 인코딩
                    encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
                    
            except zmq.Again:
                # 타임아웃이 발생한 경우 처리
                print("Error: No response from server (timeout). Exiting...")
                img_nm = find_largest_jpg_file('./img/' + out_path +'_short')
                print(img_nm)
                width, height = get_image_size('./img/' + out_path +'_short/' + img_nm)
                # print(width, height)

                #이미지 파일 로드
                with open('./img/' + out_path +'_short/' + img_nm, 'rb') as image_file:
                    encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

    if encoded_image == None:
        img_nm = find_largest_jpg_file('./img/' + out_path +'_short')
        print(img_nm)
        width, height = get_image_size('./img/' + out_path +'_short/' + img_nm)
        # print(width, height)

        #이미지 파일 로드
        with open('./img/' + out_path +'_short/' + img_nm, 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

    # height와 width의 최대공약수(GCD) 구하기

    gcd = math.gcd(width, height)

    # 비율 계산
    width_ratio = width // gcd
    height_ratio = height // gcd

    # 응답으로 JSON 반환
    response = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        'img_decode_data': encoded_image,
        'img_size': {
            'width': width,
            'height': height,
        },
        'ratio':  f"{width_ratio}:{height_ratio}"
    }
    return jsonify(response)

# grid_st 이미지 받기
@cctv_process.route('/get_grid_st_socket', methods=['POST'])
def get_grid_st_socket():
    cctv_data = request.json
    port_num = cctv_data['port_num']


    #소켓통신으로 타 프로스세
    # 에서 이미지 받기
    if port_num != '':
        if port_num != '1':
            port = port_num
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect("tcp://localhost:" + str(port))
            print("tcp://localhost:" +  str(port))

            # 응답 타임아웃 설정 (5000ms = 5초)
            socket.setsockopt(zmq.RCVTIMEO, 5000)
            
            try:
                # 서버에 검출상태 요청
                socket.send_string("send_grid_st")
                
                # 서버로부터 응답 수신
                response = socket.recv_string()
                print(f"Received data length: {len(response)}")
                
                if response.startswith("Error"):
                    print(f"Server error: {response}")
                else:
                    try:
                        grid_st_data = json.loads(response)
                    except Exception as e:
                        print(e)
                        grid_st_data = {}
                    print('grid_st_data')
                    print(grid_st_data)
                    if grid_st_data == None:
                        grid_st_data = {}
                    temp = {}
                    for key in grid_st_data.keys():
                        count = len(grid_st_data[key])
                        temp[key] = {
                            'obj_count' : count
                        }
                    grid_st_data = temp
                    
                    
            except zmq.Again:
                # 타임아웃이 발생한 경우 처리
                print("Error: No response from server (timeout). Exiting...")
                grid_st_data = {}

    # 응답으로 JSON 반환
    response = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        'grid_st_data': grid_st_data
    }
    return jsonify(response)

# 파일 받기
@cctv_process.route('/get_file', methods=['POST'])
def get_file():

    file_data = request.json

    # 경로와 파일명을 분리
    directory, file_name = os.path.split(file_data["file_path"])

    # 요청된 파일을 지정된 디렉토리에서 찾고, 전송합니다.
    return send_from_directory(directory, file_name, as_attachment=True)

#비디오 파일 전송
@cctv_process.route('/video') 
def stream_video():
    """지정된 비디오 파일을 스트리밍"""
    video_path = request.args.get("path")  # 요청된 비디오 파일 경로

    if not video_path or not os.path.exists(video_path):
        return jsonify({"error": "File not found"}), 404

    range_header = request.headers.get('Range', None)
    file_size = os.path.getsize(video_path)

    if not range_header:
        # Range 요청이 없는 경우 전체 파일 전송
        with open(video_path, 'rb') as video:
            data = video.read()
        return Response(data, status=200, mimetype="video/mp4")

    # Range 요청이 있는 경우
    start, end = range_header.replace("bytes=", "").split("-")
    start = int(start)
    end = int(end) if end else file_size - 1  # 끝 범위가 지정되지 않으면 파일 끝까지

    chunk_size = 1024 * 1024  # 1MB 단위로 전송
    end = min(end, start + chunk_size - 1)  # 1MB까지만 전송

    with open(video_path, "rb") as video:
        video.seek(start)
        data = video.read(end - start + 1)

    response = Response(data, status=206, mimetype="video/mp4")
    response.headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["Content-Length"] = str(end - start + 1)

    return response

# 파일 받기 1 
@cctv_process.route('/get_file1', methods=['POST'])
def get_file1():

    file_data = request.json
    # 경로와 파일명을 분리
    try:
        #이미지 파일
        width, height = get_image_size(file_data["file_path"])
        # print(width, height)
    except Exception as e:
        print(e)
        #동영상 파일
        try:
            width, height = get_imgfile_size(file_data["cctv_id"])
        except Exception as e:
            print(f"Error: {e}")
            width =  None
            height = None


    #이미지 파일 로드
    try:
        with open(file_data["file_path"], 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        print(e)
        current_directory = os.getcwd()
        with open(current_directory + file_data["file_path"].replace("..", ""), 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

    # height와 width의 최대공약수(GCD) 구하기
    try:
        gcd = math.gcd(width, height)
    except Exception as e:
        print(e)
        gcd = None

    # 비율 계산
    try:
        width_ratio = width // gcd
        height_ratio = height // gcd
    except Exception as e:
        print(e)
        width_ratio = None
        height_ratio = None


    # 응답으로 JSON 반환
    response = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        'img_decode_data': encoded_image,
        'img_size': {
            'width': width,
            'height': height,
        },
        'ratio':  f"{width_ratio}:{height_ratio}"
    }
    return jsonify(response)

@cctv_process.route('/terminate_process', methods=['POST'])
def terminate_process():
    global _batch_ai_process, _batch_sm_process, _batch_cam_ids, _batch_timer
    global _batch_ai_log_fh, _batch_sm_log_fh

    payload = request.get_json(silent=True) or {}
    pid = payload.get('pid')
    cctv_id = payload.get('cctv_id')

    if not pid:
        return jsonify({"success": False, "code": 400, "msg": "pid required"}), 400

    pid = int(pid)

    # jit-only
    if pid == 1:
        return jsonify({"success": True, "code": 200, "msg": "성공하였습니다.", "status": "noop", "pid": pid})

    # 배치 버퍼에 있는 카메라 제거 (아직 미실행 상태)
    if cctv_id:
        with _batch_lock:
            removed = _batch_buffer.pop(cctv_id, None)
            if removed:
                with _port_lock:
                    port_dict.pop(f"_pending_{cctv_id}", None)
                print(f"[BATCH] {cctv_id} removed from buffer", flush=True)
                if not _batch_buffer and _batch_timer:
                    _batch_timer.cancel()
                    _batch_timer = None

    # 배치 프로세스인지 확인
    is_batch = _batch_ai_process is not None and _batch_ai_process.pid == pid

    # dict 정리 (있으면)
    process_dict.pop(pid, None)
    port_dict.pop(pid, None)

    # AI 프로세스 종료
    terminated = terminate_process_tree(pid)

    # 배치 프로세스인 경우 추가 정리
    if is_batch:
        # state_manager도 종료
        if _batch_sm_process:
            try:
                terminate_process_tree(_batch_sm_process.pid)
            except Exception as e:
                print(f"[BATCH] state_manager terminate error: {e}", flush=True)

        # 로그 핸들 닫기
        for fh_name, fh in (("ai", _batch_ai_log_fh), ("sm", _batch_sm_log_fh)):
            if fh:
                try:
                    fh.close()
                except Exception as e:
                    print(f"[BATCH] {fh_name} log close error: {e}", flush=True)
        _batch_ai_log_fh = None
        _batch_sm_log_fh = None

        # 모든 배치 카메라 DB PID 초기화
        for cid in _batch_cam_ids:
            update_camera_pid(cid, '', None, False)

        # 배치 상태 초기화
        _batch_port_map.clear()
        _batch_cam_ids = []
        _batch_ai_process = None
        _batch_sm_process = None

        # 대기 중인 타이머 취소
        if _batch_timer:
            _batch_timer.cancel()
            _batch_timer = None
        with _batch_lock:
            _batch_buffer.clear()

        print(f"[BATCH] Batch terminated, all cameras cleared", flush=True)

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "status": "terminated" if terminated else "already_stopped",
        "pid": pid
    })

@cctv_process.route('/chk_pid', methods=['POST'])
def chk_pid():
    pid = request.json.get('pid')

    # 딕셔너리에서 프로세스 가져오기
    process = process_dict.get(pid)

    # print(process)

    if process:
        # 프로세스 존제함
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            'pid exists': True})
    else:
        # 프로세스 프로세스 없음.
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            'pid exists': False})
