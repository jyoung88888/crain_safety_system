import base64
from flask import Blueprint, request, jsonify

# 블루프린트 생성, /cctv/log 상위 경로 추가
cctv_log = Blueprint('cctv_log', __name__, url_prefix='/cctv/log')

# 동영상 받기
@cctv_log.route('/get_test_video', methods=['GET'])
def get_test_video():
    # 이미지 파일 로드
    with open('./video/test.mp4', 'rb') as video_file:
        encoded_video = base64.b64encode(video_file.read()).decode('utf-8')

    # 응답으로 JSON 반환
    response = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "file_name": "test.mp4",
        'video_decode_data': encoded_video
    }
    return jsonify(response)
    # try:
    #     # 요청된 파일을 지정된 디렉토리에서 찾고, 전송합니다.
    #     return send_from_directory('./video', 'test.mp4', as_attachment=True)
    # except FileNotFoundError:
    #     return jsonify({'error': 'File not found'}), 404