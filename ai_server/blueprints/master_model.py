from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import insert_tb_camera_ai_model, update_tb_camera_ai_model, load_data, \
    MODEL_DATA_FILE, save_data, get_tb_camera_ai_model_by_name, get_all_tb_camera_ai_models, delete_tb_camera_ai_model

# 모델 마스터
# 블루프린트 생성, /cctv/model_crud 상위 경로 추가
cctv_model = Blueprint('cctv_model_crud', __name__, url_prefix='/cctv/model_crud')

# 모델 데이터 추가
@cctv_model.route('/model', methods=['POST'])
def model():
    # data = load_data(MODEL_DATA_FILE)
    # new_id = int(max(data.keys(), default=0)) + 1
    # model_data = request.json
    # data[new_id] = model_data
    # save_data(data, MODEL_DATA_FILE)
    model_data = request.json
    print(model_data)
    add_count = 0
    add_arr = []
    update_count = 0
    update_arr = []
    for model in model_data:
        try:
            __created__ = model["__created__"]
        except Exception as e:
            __created__ = False

        parts = model['userCd'].split('_', 1)
        # comp_id = get_comp_id_by_user_cd(server['userCd'])
        comp_id = parts[0]
        userCd = parts[1]

        if __created__:
            result = insert_tb_camera_ai_model(model['model_nm'],
                                               model['model_txt'],
                                               userCd)
            if result != None:
                add_arr.append(result)
                add_count += 1
        else:
            result = update_tb_camera_ai_model(model['model_nm'],
                                               model['model_txt'],
                                               userCd)
            if result != None:
                update_arr.append(model['model_nm'])
                update_count += 1
    if add_count != 0 or update_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "model added successfully",
            "add_count": add_count, "add_arr": add_arr,
            "update_count": update_count, "update_arr": update_arr})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "model added fail"})


# 모델 데이터 수정
@cctv_model.route('/model/<int:model_id>', methods=['PUT'])
def update_model(model_id):
    data = load_data(MODEL_DATA_FILE)
    if str(model_id) not in data:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "model not found"})
    model_data = request.json
    data[str(model_id)] = model_data
    print(model_data)
    save_data(data, MODEL_DATA_FILE)
    return jsonify({"success": False,
                    "code": 404,
                    "msg": "model updated successfully"})


# 모델 데이터 조회 (특정 아이템 조회)
@cctv_model.route('/model/<string:model_id>', methods=['GET'])
def get_modle(model_id):
    # data = load_data(MODEL_DATA_FILE)
    # model = data.get(str(model_id))
    # if not model:
    #     return jsonify({"message": "model not found"}), 404
    #     #형식 변경
    # output_data = {
    #     "data": model
    # }
    model = get_tb_camera_ai_model_by_name(model_id)
    if not model:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "model not found"})
    # 형식 변경
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": {
            "model_nm": model[0],
            "model_txt": model[1],
            "created_at": model[2].strftime("%Y.%m.%d %H:%M:%S") if model[2] != None else None,
            "created_by": model[3],
            "updated_at": model[4].strftime("%Y.%m.%d %H:%M:%S") if model[4] != None else None,
            "updated_by": model[5],
        }
    }

    return jsonify(output_data)


# 모델 전체 조회
@cctv_model.route('/models', methods=['GET'])
def get_all_model():
    # data = load_data(MODEL_DATA_FILE)

    # #형식 변경
    # output_data = {
    #                 "list": [
    #                     {
    #                         "id": key,
    #                         "model_nm": value["model_nm"],
    #                         "model_txt": value["model_txt"]
    #                     } for key, value in data.items()
    #                 ]
    # }
    data = get_all_tb_camera_ai_models()
    list = [
        {
            "model_nm": value[0],
            "model_txt": value[1],
            "created_at": value[2].strftime("%Y.%m.%d %H:%M:%S") if value[2] != None else None,
            "created_by": value[3],
            "updated_at": value[4].strftime("%Y.%m.%d %H:%M:%S") if value[4] != None else None,
            "updated_by": value[5],
        } for value in data
    ]
    list = sorted(list, key=lambda x: x['model_nm'])
    # 형식 변경
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# 모델 데이터 삭제
@cctv_model.route('/model', methods=['DELETE'])
def delete_model():
    # data = load_data(MODEL_DATA_FILE)
    # if str(model_id) not in data:
    #     return jsonify({"message": "pro_detail not found"}), 404
    # del data[str(model_id)]
    # save_data(data, MODEL_DATA_FILE)
    model_data = request.json
    count = 0
    del_lst = []
    for model_nm in model_data:
        result = delete_tb_camera_ai_model(model_nm)
        if result != False:
            del_lst.append(model_nm)
            count += 1
    if count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": f"model deleted successfully", "count": count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "model not found"})
