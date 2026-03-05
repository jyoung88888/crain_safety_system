from flask import Blueprint, request, jsonify, send_file
import os
import json
from blueprints.lib.public_func import get_connection, get_camera_by_id, get_camera_by_id1, get_ai_server_info
import requests

# 0. BluePrint
# Blueprint 정의
dt_crud_remote = Blueprint('dt_crud_remote', __name__, url_prefix='/cctv/dt_crud_remote')

# 1. 전역 상태 관리
# 이미지 경로 설정
current_directory = os.getcwd()
img_path = os.path.join(current_directory, 'img')



# 2. 초기화 및 기본 설정
@dt_crud_remote.route('/get_remote_server_cap/<string:camera_id>', methods=['GET'])
def get_remote_server_cap(camera_id):
    """
    Fetches disk usage information from a remote server based on camera_id.
    """
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_info = get_ai_server_info(camera_info['ai_server_id'])
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/get_server_cap"

    print(f"Requesting URL: {url}")  # 요청 URL 출력

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.get(url)
        if response.status_code == 200:
            return jsonify({"message": "Successfully fetched server capacity from remote server", "data": response.json(), "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to fetch server capacity from remote server", "details": response.text, "success": False, "code": response.status_code}), response.status_code
    except Exception as e:
        return jsonify({"error": "An error occurred while contacting the remote server", "details": str(e), "success": False, "code": 500}), 500

@dt_crud_remote.route('/get_remote_data_state/<string:camera_id>', methods=['POST'])
def get_remote_data_state(camera_id):
    """
    Fetches data state from a remote server based on camera_id and collection_period.
    """
    camera_info = get_camera_by_id1(camera_id)

    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_id = camera_info['ai_server_id']
    if isinstance(ai_server_id, list):
        ai_server_id = ai_server_id[0]

    ai_server_info = get_ai_server_info(ai_server_id)
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    data = request.get_json()
    if not data or 'collection_period' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']


    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/get_data_state"
    payload = {"camera_id": camera_id, "collection_period": collection_period}

    print(f"Requesting URL: {url}")  # 요청 URL 출력
    print(f"payload: {payload}")  # 요청 URL 출력

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        # 타임아웃 설정 추가 (10초)
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            return jsonify({"message": "Successfully fetched data state from remote server", "data": response.json(), "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to fetch data state from remote server", "details": response.text, "success": False, "code": response.status_code}), response.status_code
    except requests.exceptions.Timeout:
        return jsonify({"error": "Request timed out while contacting the remote server", "success": False, "code": 408}), 408
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "An error occurred while fetching data state from the remote server", "details": str(e), "success": False, "code": 500}), 500

@dt_crud_remote.route('/delete_remote_images/<string:camera_id>', methods=['POST'])
def delete_remote_images(camera_id):
    """
    Deletes images on a remote server based on camera_id and collection_period.
    """
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_id = camera_info['ai_server_id']
    if isinstance(ai_server_id, list):
        ai_server_id = ai_server_id[0]

    ai_server_info = get_ai_server_info(ai_server_id)

    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    data = request.get_json()
    if not data or 'collection_period' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']

    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/del_img"
    payload = {"camera_id": camera_id, "collection_period": collection_period}

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            return jsonify({"message": "Successfully deleted images on remote server", "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to delete images on remote server", "details": response.text, "success": False, "code": response.status_code}), response.status_code
    except Exception as e:
        return jsonify({"error": "An error occurred while deleting images on the remote server", "details": str(e), "success": False, "code": 500}), 500

@dt_crud_remote.route('/create_remote_zip/<string:camera_id>', methods=['POST'])
def create_remote_zip(camera_id):
    """
    Creates a zip file on a remote server based on camera_id and collection_period.
    """
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_info = get_ai_server_info(camera_info['ai_server_id'])
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    data = request.get_json()
    if not data or 'collection_period' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']

    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/create_zip"
    payload = {"camera_id": camera_id, "collection_period": collection_period}

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            return jsonify({"message": "Successfully created zip on remote server", "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to create zip on remote server", "details": response.text, "success": False, "code": 200}), 200
    except Exception as e:
        return jsonify({"error": "An error occurred while creating zip on the remote server", "details": str(e), "success": False, "code": 500}), 500


@dt_crud_remote.route('/get_remote_zip_list/<string:camera_id>', methods=['POST'])
def get_remote_zip_list(camera_id):
    """
    Fetches a list of zip files from a remote server based on camera_id and collection_period.
    """
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_info = get_ai_server_info(camera_info['ai_server_id'])
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    out_path = camera_info['out_path']
    pid = camera_info['pid']
    rtsp_addr = camera_info['rtsp_addr']
    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    data = request.get_json()
    if not data or 'collection_period' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']

    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/get_zip_list"
    payload = {"camera_id": camera_id, "collection_period": collection_period}

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            return jsonify({"message": "Successfully fetched zip list from remote server", "data": response.json(), "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to fetch zip list from remote server", "details": response.text, "success": False, "code": 200}), 200
    except Exception as e:
        return jsonify({"error": "An error occurred while contacting the remote server", "details": str(e), "success": False, "code": 500}), 500


@dt_crud_remote.route('/download_remote_zip/<string:camera_id>', methods=['POST'])
def download_remote_zip(camera_id):
    """
    Streams a zip file from a remote server directly to the client without saving it to the server's disk.
    """
    from flask import Response

    # 카메라 정보 가져오기
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    # AI 서버 정보 가져오기
    ai_server_info = get_ai_server_info(camera_info['ai_server_id'])
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'collection_period' not in data or 'file_name' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']
    file_name = data['file_name']

    # 원격 서버 URL 설정
    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/download_zip_list"
    payload = {"camera_id": camera_id, "collection_period": collection_period, "file_name": file_name}

    # 원격 서버에서 파일 스트리밍
    try:
        response = requests.post(url, json=payload, stream=True, timeout=120)

        if response.status_code != 200:
            return jsonify({
                "error": "Failed to download zip file from remote server",
                "details": response.text,
                "success": False,
                "code": response.status_code
            }), response.status_code

        # 파일 스트리밍 생성
        def generate_remote_file_chunks():
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    yield chunk

        # 응답 헤더 구성
        headers = {
            'Content-Disposition': f'attachment; filename={file_name}',
            'Content-Length': response.headers.get('Content-Length', '0'),
            'Content-Type': 'application/octet-stream',
        }

        # 클라이언트로 파일 스트리밍
        return Response(generate_remote_file_chunks(), headers=headers, content_type='application/octet-stream')

    except requests.exceptions.Timeout:
        return jsonify({"error": "Request timed out while contacting the remote server", "success": False, "code": 408}), 408
    except requests.exceptions.RequestException as e:
        return jsonify({"error": "An error occurred while downloading from the remote server", "details": str(e), "success": False, "code": 500}), 500





@dt_crud_remote.route('/delete_remote_zip/<string:camera_id>', methods=['POST'])
def delete_remote_zip(camera_id):
    """
    Deletes a zip file on a remote server based on camera_id and collection_period.
    """
    camera_info = get_camera_by_id1(camera_id)
    if not camera_info:
        return jsonify({"error": "Camera info not found for the given camera_id", "success": False, "code": 404}), 404

    ai_server_info = get_ai_server_info(camera_info['ai_server_id'])
    if not ai_server_info:
        return jsonify({"error": "AI server info not found for the given ai_server_id", "success": False, "code": 404}), 404

    data = request.get_json()
    if not data or 'collection_period' not in data or 'file_name' not in data:
        return jsonify({"error": "Missing required parameters", "success": False, "code": 400}), 400

    collection_period = data['collection_period']
    file_name = data['file_name']
    server_host = ai_server_info['server_host']
    api_port = ai_server_info['api_port']

    url = f"http://{server_host}:{api_port}/dt_manage/dt_crud/del_zip_list"
    payload = {"camera_id": camera_id, "collection_period": collection_period, "file_name": file_name}

    print(f"Requesting URL: {url}")  # 요청 URL 출력
    print(f"payload: {payload}")  # 요청 URL 출력

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            return jsonify({"message": f"Successfully deleted {file_name} on remote server", "success": True, "code": 200}), 200
        else:
            return jsonify({"error": "Failed to delete zip file on remote server", "details": response.text, "success": False, "code": response.status_code}), response.status_code
    except Exception as e:
        return jsonify({"error": "An error occurred while deleting on the remote server", "details": str(e), "success": False, "code": 500}), 500