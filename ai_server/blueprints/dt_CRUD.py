from flask import Blueprint, request, jsonify, send_file
import psutil
import os
import zipfile
from datetime import datetime

# 0. BluePrint
# Blueprint 정의
dt_crud = Blueprint('dt_crud', __name__, url_prefix='/dt_manage/dt_crud')

# 1. 전역 상태 관리
# 이미지 경로 설정
current_directory = os.getcwd()
img_path = os.path.join(current_directory, 'img')


# 2. 초기화 및 기본 설정
@dt_crud.route('/get_server_cap', methods=['GET'])
def disk_usage():
    # Disk usage
    disk = psutil.disk_usage('/')
    disk_total = disk.total / (1024 ** 4)  # Convert to TB
    disk_used = disk.used / (1024 ** 4)  # Convert to TB
    disk_free = disk.free / (1024 ** 4)  # Convert to TB
    disk_percent = disk.percent  # Usage percentage remains as is

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "disk": {
            "total_tb": round(disk_total, 2),  # Total in TB, rounded to 2 decimals
            "used_tb": round(disk_used, 2),  # Used in TB, rounded to 2 decimals
            "free_tb": round(disk_free, 2),  # Free in TB, rounded to 2 decimals
            "percent": disk_percent  # Usage percentage
        }
    }), 200


@dt_crud.route('/get_data_state', methods=['POST'])
def get_data_state():
    import logging
    logging.basicConfig(level=logging.ERROR)

    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data:
        return jsonify({"error": "Missing camera_id or collection_period", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']

    # Validate input to prevent directory traversal attacks
    import re
    if not re.match(r'^[\w-]+$', camera_id) or not re.match(r'^[\w-]+$', collection_period):
        return jsonify({"error": "Invalid camera_id or collection_period", "success": False, "code": 400}), 400

    target_folder = os.path.join('img', f"{camera_id}_{collection_period}")

    if not os.path.exists(target_folder):
        return jsonify({"msg":  f"Folder not found for {camera_id}_{collection_period}", "success": True, "code": 200}), 200
        # return jsonify({"error": f"Folder not found for {camera_id}_{collection_period}"}), 404

    # 폴더 내 이미지 파일 목록 가져오기 (zip 파일 제외)
    files = [f for f in os.listdir(target_folder) if
             os.path.isfile(os.path.join(target_folder, f)) and not f.endswith('.zip')]
    if not files:
        return jsonify({"msg": "현재 수집된 데이터 없음", "success": True, "code": 200}), 200

    timestamps = []
    for file in files:
        file_path = os.path.join(target_folder, file)
        try:
            creation_time = os.path.getctime(file_path)
            timestamps.append(creation_time)
        except Exception as e:
            logging.error(f"Error processing file {file}: {e}")

    if not timestamps:
        return jsonify({"msg": "현재 수집된 데이터 없음", "success": True, "code": 200}), 200

    start_collect = datetime.fromtimestamp(min(timestamps)).strftime("%Y-%m-%d %H:%M:%S")
    end_collect = datetime.fromtimestamp(max(timestamps)).strftime("%Y-%m-%d %H:%M:%S")
    total_img = len(files)

    return jsonify(
        {
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "start_collect": start_collect,
            "end_collect": end_collect,
            "total_img": total_img
        })


@dt_crud.route('/create_zip', methods=['POST'])
def create_zip():
    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data:
        return jsonify({"error": "Missing camera_id or collection_period", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']
    target_folder = os.path.join(img_path, f"{camera_id}_{collection_period}")
    
    
    print('target_folder', target_folder)
    if not os.path.exists(target_folder):
        return jsonify(
            {"error": f"Folder not found for {camera_id}_{collection_period}", "success": False, "code": 404}), 404

    # 폴더 내 이미지 파일 목록 가져오기
    files = [f for f in os.listdir(target_folder) if os.path.isfile(os.path.join(target_folder, f))]
    if not files:
        return jsonify({"error": "No images found", "success": False, "code": 404}), 404
    
    files = [f for f in files if not f.lower().endswith('.zip')]


    # 가장 오래된 파일 생성 시각과 최신 시각 가져오기
    timestamps = [datetime.fromtimestamp(os.path.getctime(os.path.join(target_folder, f))) for f in files]
    start_collect = min(timestamps).strftime("%Y%m%d%H%M%S")
    end_collect = datetime.now().strftime("%Y%m%d%H%M%S")  # 현재 시간 사용

    # 압축 파일명 생성 (collection_period에 따른 파일명 변경)
    if collection_period == 'long':
        zip_file_name = f"backup_{camera_id}_60m_{start_collect}_{end_collect}.zip"
    else:  # short
        zip_file_name = f"backup_{camera_id}_10m_{start_collect}_{end_collect}.zip"

    zip_file_path = os.path.join(target_folder, zip_file_name)  # 압축 파일을 동일한 폴더에 생성

    # 압축 파일 생성
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file in files:
            zipf.write(os.path.join(target_folder, file), file)

    # 압축 파일 정보
    zip_file_size_bytes = os.path.getsize(zip_file_path)  # 파일 크기 (바이트)
    zip_file_size_gb = round(zip_file_size_bytes / (1024 ** 3), 2)  # GB 단위로 변환, 소수점 둘째 자리 반올림
    zip_creation_time = datetime.now()

    # 압축 생성 시점 이전의 이미지 파일 삭제
    for file in files:
        file_path = os.path.join(target_folder, file)
        file_creation_time = datetime.fromtimestamp(os.path.getctime(file_path))
        if file_creation_time < zip_creation_time:  # 압축 생성 시점 이전의 파일 삭제
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Failed to delete file {file_path}: {e}")

    # 결과 반환
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": "Zip file created and saved successfully. Older files have been deleted.",
        "zip_file_name": zip_file_name,
        "creation_date": zip_creation_time.strftime("%Y-%m-%d %H:%M:%S"),
        "file_size_gb": zip_file_size_gb  # GB 단위로 반환
    })


@dt_crud.route('/del_img', methods=['POST'])
def del_img():
    """
    Deletes all images for a given camera_id and collection_period, excluding zip files.
    """
    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data:
        return jsonify({"error": "Missing camera_id or collection_period", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']
    target_folder = os.path.join(img_path, f"{camera_id}_{collection_period}")

    if not os.path.exists(target_folder):
        return jsonify(
            {"error": f"Folder not found for {camera_id}_{collection_period}", "success": False, "code": 404}), 404

    # 폴더 내 이미지 파일 목록 가져오기 (zip 파일 제외)
    try:
        files = [os.path.join(target_folder, f) for f in os.listdir(target_folder) if
                 os.path.isfile(os.path.join(target_folder, f)) and not f.endswith('.zip')]
        if not files:
            return jsonify({"message": f"No images found in folder {target_folder}", "success": True, "code": 200}), 200

        for file in files:
            try:
                os.remove(file)
            except Exception as e:
                print(f"Failed to delete file {file}: {e}")
                return jsonify({"error": f"Failed to delete some files: {str(e)}", "success": False, "code": 500}), 500
    except Exception as e:
        print(f"Error during file deletion: {e}")
        return jsonify({"error": "An unexpected error occurred during deletion", "details": str(e), "success": False,
                        "code": 500}), 500

    # 결과 반환
    return jsonify({
        "message": f"All images in folder {target_folder} have been successfully deleted, excluding zip files",
        "success": True, "code": 200, "msg": "성공하였습니다."
    }), 200


@dt_crud.route('/get_zip_list', methods=['POST'])
def get_zip_list():
    """
    Returns a list of zip files with their creation date, name, and size for a given camera_id and collection_period.
    """
    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data:
        return jsonify({"error": "Missing camera_id or collection_period", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']
    target_folder = os.path.join(img_path, f"{camera_id}_{collection_period}")

    if not os.path.exists(target_folder):
        return jsonify(
            {"error": f"Folder not found for {camera_id}_{collection_period}", "success": False, "code": 200}), 200

    # 폴더 내 zip 파일 목록 가져오기
    zip_files = [f for f in os.listdir(target_folder) if
                 os.path.isfile(os.path.join(target_folder, f)) and f.endswith('.zip')]
    if not zip_files:
        return jsonify({"message": "No zip files found in folder", "success": True, "code": 200}), 200

    zip_list = []
    for zip_file in zip_files:
        zip_path = os.path.join(target_folder, zip_file)
        creation_time = datetime.fromtimestamp(os.path.getctime(zip_path)).strftime("%Y-%m-%d %H:%M:%S")
        file_size_gb = round(os.path.getsize(zip_path) / (1024 ** 3), 2)  # GB 단위로 변환, 소수점 둘째 자리 반올림
        zip_list.append({
            "file_name": zip_file,
            "create_zip_date": creation_time,
            "file_size_gb": file_size_gb
        })

    # 결과 반환
    return jsonify({"zip_files": zip_list, "success": True, "code": 200, "msg": "성공하였습니다."}), 200


@dt_crud.route('/download_zip_list', methods=['POST'])
def download_zip_list():
    """
    Allows a client to download a selected zip file from the server with improved handling for large files.
    """
    from flask import Response
    import math

    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data or 'file_name' not in data:
        return jsonify(
            {"error": "Missing camera_id, collection_period, or file_name", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']
    file_name = data['file_name']

    target_folder = os.path.join(img_path, f"{camera_id}_{collection_period}")
    zip_file_path = os.path.join(target_folder, file_name)

    if not os.path.exists(zip_file_path):
        return jsonify(
            {"error": f"Zip file {file_name} not found in folder {target_folder}", "success": False, "code": 404}), 404

    if not os.path.isfile(zip_file_path):
        return jsonify({"error": f"{file_name} is not a valid file", "success": False, "code": 400}), 400

    # 예상 다운로드 시간 계산
    file_size_bytes = os.path.getsize(zip_file_path)
    file_size_gb = round(file_size_bytes / (1024 ** 3), 2)
    estimated_time_minutes = max(1, int(file_size_gb))  # 최소 1분, 파일 크기 기준 계산

    # 다운로드 시작 메시지 출력
    print(f"Download of {file_name} has started. Estimated time: {estimated_time_minutes} minutes.")
    start_time = datetime.now()

    # 대용량 파일 스트리밍 처리
    def generate_large_file_chunks(file_path, chunk_size=8192):
        with open(file_path, 'rb') as file:
            while chunk := file.read(chunk_size):
                yield chunk

    # HTTP 응답 생성
    try:
        headers = {
            'Content-Disposition': f'attachment; filename={file_name}',
            'Content-Length': str(file_size_bytes),
        }
        return Response(
            generate_large_file_chunks(zip_file_path),
            headers=headers,
            content_type='application/octet-stream',
        )
    except Exception as e:
        print(f"Failed to send file: {e}")
        return jsonify({"error": "Failed to send file", "details": str(e), "success": False, "code": 500}), 500



@dt_crud.route('/del_zip_list', methods=['POST'])
def del_zip_list():
    """
    Deletes a specified zip file from the server based on camera_id, collection_period, and file_name.
    """
    # 요청 데이터 가져오기
    data = request.get_json()
    if not data or 'camera_id' not in data or 'collection_period' not in data or 'file_name' not in data:
        return jsonify(
            {"error": "Missing camera_id, collection_period, or file_name", "success": False, "code": 400}), 400

    camera_id = data['camera_id']
    collection_period = data['collection_period']
    file_name = data['file_name']

    target_folder = os.path.join(img_path, f"{camera_id}_{collection_period}")
    zip_file_path = os.path.join(target_folder, file_name)

    if not os.path.exists(zip_file_path):
        return jsonify(
            {"error": f"Zip file {file_name} not found in folder {target_folder}", "success": False, "code": 404}), 404

    if not os.path.isfile(zip_file_path):
        return jsonify({"error": f"{file_name} is not a valid file", "success": False, "code": 400}), 400

    # 파일 삭제 시도
    try:
        os.remove(zip_file_path)
        return jsonify({"message": f"Zip file {file_name} has been successfully deleted.", "success": True, "code": 200,
                        "msg": "성공하였습니다."}), 200
    except Exception as e:
        print(f"Failed to delete file: {e}")
        return jsonify({"error": "Failed to delete file", "details": str(e), "success": False, "code": 500}), 500
