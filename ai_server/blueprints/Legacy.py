from flask import Blueprint, request, jsonify

from blueprints.lib.public_func import fn_get_legacy_bwts_runing_work, fn_get_legacy_scrubber_runing_work, fn_set_pro_oder_link, fn_set_input_oder_link, fn_get_legacy_scrubber_at_ordnum

# 서버 정보
# 블루프린트 생성, /cctv/sim 상위 경로 추가
legacy = Blueprint('legacy', __name__, url_prefix='/cctv/legacy')


# 수주정보로 조회
@legacy.route('/get_legacy_scrubber_at_ordnum/<string:ordnum>', methods=['GET'])
def get_legacy_scrubber_at_ordnum(ordnum):
    data = fn_get_legacy_scrubber_at_ordnum(ordnum)
    return jsonify(data.to_dict(orient="records"))



# 현재 작업중인 bwts 제품 list
@legacy.route('/get_legacy_bwts_runing_work', methods=['GET'])
def get_legacy_bwts_runing_work():
    data = fn_get_legacy_bwts_runing_work()
    return jsonify(data.to_dict(orient="records"))



# 현재 작업중인 scrubber 제품 list
@legacy.route('/get_legacy_scrubber_runing_work', methods=['GET'])
def get_legacy_scrubber_runing_work():
    data = fn_get_legacy_scrubber_runing_work()
    return jsonify(data.to_dict(orient="records"))

# 공정 진행율 데이터 <-> 수주정보 연결 REST API
@legacy.route('/set_pro_oder_link', methods=['POST'])
def set_pro_oder_link():
    server_data = request.json

    image_id = server_data['image_id']
    detection_id = server_data['detection_id']
    ordnum = server_data['ordnum']
    ordseq = server_data['ordseq']

    data = fn_set_pro_oder_link(image_id, detection_id, ordnum, ordseq)
    if data == True:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "변경 성공하였습니다.",
         })
    else: 
        return jsonify({
            "success": False,
            "code": 500,
            "msg": "변경 실패하였습니다.",
         })
    
# 사용자 입력 데이터 <-> 수주정보 연결 REST API
@legacy.route('/set_input_oder_link', methods=['POST'])
def set_input_oder_link():
    server_data = request.json

    image_id = server_data['image_id']
    detection_id = server_data['detection_id']
    ordnum = server_data['ordnum']
    ordseq = server_data['ordseq']

    data = fn_set_input_oder_link(image_id, detection_id, ordnum, ordseq)
    if data == True:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "변경 성공하였습니다.",
         })
    else: 
        return jsonify({
            "success": False,
            "code": 500,
            "msg": "변경 실패하였습니다.",
         })