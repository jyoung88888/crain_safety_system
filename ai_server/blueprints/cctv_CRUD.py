from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import insert_camera, get_all_tb_camera_ai_models, insert_camera_roi, update_camera, \
    get_camera_by_id, get_all_cameras, get_camera_by_comp_id, delete_camera_roi, delete_camera, get_camera_server_by_id

# cctv 정보
# 블루프린트 생성, /cctv/cctv_crud 상위 경로 추가
cctv_crud = Blueprint('cctv_crud', __name__, url_prefix='/cctv/cctv_crud')

# cctv 데이터 추가
@cctv_crud.route('/cctv', methods=['POST'])
def add_cctv():
    cctv_data = request.json
    add_count = 0
    add_arr = []
    update_count = 0
    update_arr = []

    for cctv in cctv_data:
        try:
            __created__ = cctv["__created__"]
        except Exception as e:
            __created__ = False

        try:
            comp_id = cctv['comp_id']
            parts = cctv['userCd'].split('_', 1)
            userCd = parts[1]
        except Exception as e:
            parts = cctv['userCd'].split('_', 1)
            # comp_id = get_comp_id_by_user_cd(server['userCd'])
            comp_id = parts[0]
            userCd = parts[1]

        if __created__:
            result = insert_camera(comp_id, cctv["cctv_name"],
                                   cctv["camera_desc"], cctv["server_id"],
                                   cctv["rtsp_add"],
                                   cctv["pid"],  cctv["jit_only"],
                                   cctv["remark"], cctv["location"],
                                   userCd)
            if result != None:
                # roi 내용 추가
                model_data = get_all_tb_camera_ai_models()
                model_lst = []
                for model in model_data:
                    model_lst.append(model[0])

                # for model_nm in model_lst:
                #     insert_camera_roi(result, [], model_nm, False, userCd)

                add_arr.append(result)
                add_count += 1
        else:
            result = update_camera(cctv["cctv_id"], comp_id, cctv["cctv_name"],
                                   cctv["camera_desc"], cctv["server_id"],
                                   cctv["rtsp_add"],
                                   cctv["pid"], cctv["jit_only"],
                                   cctv["remark"],
                                   cctv["location"],
                                   userCd)
            if result != None:
                update_arr.append(cctv["cctv_id"])
                update_count += 1

    if add_count != 0 or update_count != 0:
        return jsonify({"success": True,
                        "code": 200,
                        "msg": "성공하였습니다.",
                        "add_count": add_count, "add_arr": add_arr,
                        "update_count": update_count, "update_arr": update_arr})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "cctv added fail"})


# cctv 데이터 수정
@cctv_crud.route('/cctv/<string:cctv_id>', methods=['PUT'])
def update_cctv(cctv_id):
    # data = load_data(CCTV_DATA_FILE)
    # if str(cctv_id) not in data:
    #     return jsonify({"message": "cctv not found"}), 404
    cctv_data = request.json
    # data[str(cctv_id)] = cctv_data
    # save_data(data, CCTV_DATA_FILE)
    result = update_camera(cctv_id, cctv_data["comp_id"], cctv_data["cctv_name"],
                           cctv_data["camera_desc"], cctv_data["server_id"],
                           cctv_data["rtsp_add"],
                           cctv_data["pid"],  cctv_data["jit_only"],
                           cctv_data["remark"], cctv_data["location"],
                           cctv_data["userCd"])

    if result != False:
        return jsonify({"success": True,
                        "code": 200,
                        "message": "cctv updated successfully"})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "cctv not found"}), 404


# cctv 데이터 조회 (특정 아이템 조회)
@cctv_crud.route('/cctv/<string:cctv_id>', methods=['GET'])
def get_cctv(cctv_id):
    # data = load_data(CCTV_DATA_FILE)
    # cctv = data.get(str(cctv_id))
    cctv = get_camera_by_id(cctv_id)
    if not cctv:
        return jsonify({"message": "cctv not found"}), 404
    # Transforming the input data to the desired output format
    cctv = {
        "comp_id": cctv[0],
        "cctv_id": cctv[1],
        "cctv_name": cctv[2],
        "camera_desc": cctv[3],
        "server_id": cctv[4],
        "rtsp_add": cctv[5],
        "out_path": cctv[6],
        "pid": cctv[7],
        "jit_only": cctv[13],
        "location": cctv[15],
        "remark": cctv[8],
        "created_at": cctv[9].strftime("%Y.%m.%d %H:%M:%S") if cctv[9] != None else None,
        "created_by": cctv[10],
        "updated_at": cctv[11].strftime("%Y.%m.%d %H:%M:%S") if cctv[11] != None else None,
        "updated_by": cctv[12]
    }

    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": cctv
    }

    return jsonify(output_data)

# cctv 데이터 조회 (특정 아이템 조회)
@cctv_crud.route('/cctv_server/<string:cctv_id>', methods=['GET'])
def get_cctv_server(cctv_id):
    # data = load_data(CCTV_DATA_FILE)
    # cctv = data.get(str(cctv_id))
    cctv = get_camera_server_by_id(cctv_id)
    if not cctv:
        return jsonify({"message": "cctv not found"}), 404
    # Transforming the input data to the desired output format
    cctv = {
        "cctv_id": cctv[0],
        "server_host": cctv[1],
        "api_port": cctv[2]
    }

    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": cctv
    }

    return jsonify(output_data)



# cctv 데이터 전체 조회
@cctv_crud.route('/cctvs', methods=['GET'])
def get_all_cctvs():
    # data = load_data(CCTV_DATA_FILE)
    # Transforming the input data to the desired output format
    data = get_all_cameras()
    list = [
        {
            "comp_id": cctv[0],
            "cctv_id": cctv[1],
            "cctv_name": cctv[2],
            "camera_desc": cctv[3],
            "server_id": cctv[4],
            "rtsp_add": cctv[5],
            "out_path": cctv[6],
            "pid": cctv[7],
            "jit_only": cctv[13],
            "remark": cctv[8],
            "location": cctv[15],
            "created_at": cctv[9].strftime("%Y.%m.%d %H:%M:%S") if cctv[9] != None else None,
            "created_by": cctv[10],
            "updated_at": cctv[11].strftime("%Y.%m.%d %H:%M:%S") if cctv[11] != None else None,
            "updated_by": cctv[12]
        } for cctv in data
    ]
    list = sorted(list, key=lambda x: x['cctv_name'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }
    return jsonify(output_data)


# cctv 데이터 전체 조회
@cctv_crud.route('/cctvs/<string:userCd>', methods=['GET'])
def get_all_cctvs1(userCd):
    # data = load_data(CCTV_DATA_FILE)
    # Transforming the input data to the desired output format

    parts = userCd.split('_', 1)

    if parts[0] == 'IGNS':
        data = get_all_cameras()
    else:
        data = get_camera_by_comp_id(parts[0])

    list = [
        {
            "comp_id": cctv[0],
            "cctv_id": cctv[1],
            "cctv_name": cctv[2],
            "camera_desc": cctv[3],
            "server_id": cctv[4],
            "rtsp_add": cctv[5],
            "out_path": cctv[6],
            "pid": cctv[7],
            "jit_only": cctv[13],
            "remark": cctv[8],
            "location": cctv[15],
            "created_at": cctv[9].strftime("%Y.%m.%d %H:%M:%S") if cctv[9] != None else None,
            "created_by": cctv[10],
            "updated_at": cctv[11].strftime("%Y.%m.%d %H:%M:%S") if cctv[11] != None else None,
            "updated_by": cctv[12]
        } for cctv in data
    ]
    list = sorted(list, key=lambda x: x['cctv_name'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }
    return jsonify(output_data)


# cctv 데이터 삭제
@cctv_crud.route('/cctv', methods=['DELETE'])
def delete_cctv():
    # data = load_data(CCTV_DATA_FILE)
    # if str(cctv_id) not in data:
    #     return jsonify({"message": "cctv not found"}), 404
    # del data[str(cctv_id)]
    # save_data(data, CCTV_DATA_FILE)
    cctv_data = request.json
    count = 0
    del_lst = []
    for cctv_id in cctv_data:
        # roi 설정 내용 삭제
        model_data = get_all_tb_camera_ai_models()
        model_lst = []
        for model in model_data:
            model_lst.append(model[0])

        for model_nm in model_lst:
            delete_camera_roi(cctv_id, model_nm)

        result = delete_camera(cctv_id)
        if result != False:
            del_lst.append(cctv_id)
            count += 1
    if count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": f"cctv deleted successfully", "count": count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "cctv not found"}), 404
