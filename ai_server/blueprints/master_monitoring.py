from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import insert_camera_monitoring_layout, update_camera_monitoring_layout, \
    PRO_DETAIL_DATA_FILE, load_data, save_data, get_camera_monitoring_layout_by_monitoring_grp_id, get_play_url, \
    get_ratio, get_camera_monitoring_layout_by_id, get_all_camera_monitoring_layouts, delete_camera_monitoring_layout, \
    serialize_datetime, sanitize_ratio

# 모니터링 프로필 마스터
# 블루프린트 생성, /cctv/pro_detail_crud 상위 경로 추가
cctv_pro_detail = Blueprint('cctv_pro_detail', __name__, url_prefix='/cctv/pro_detail_crud')

# 프로필 디테일 데이터 추가
@cctv_pro_detail.route('/pro_detail', methods=['POST'])
def pro_detail():
    # data = load_data(PRO_DETAIL_DATA_FILE)
    # new_id = int(max(data.keys(), default=0)) + 1
    # pro_detail_data = request.json
    # data[new_id] = pro_detail_data
    # save_data(data, PRO_DETAIL_DATA_FILE)

    pro_detail_data = request.json

    add_count = 0
    add_arr = []
    update_count = 0
    update_arr = []
    for pro_detail in pro_detail_data:

        try:
            __created__ = pro_detail["__created__"]
        except Exception as e:
            __created__ = False

        try:
            comp_id = pro_detail['comp_id']
        except Exception as e:
            parts = pro_detail['userCd'].split('_', 1)
            comp_id = parts[0]
            userCd = parts[1]

        print('comp_id: ', comp_id)

        if __created__:
            result = insert_camera_monitoring_layout(pro_detail['profile_id'],
                                                     pro_detail['x'], pro_detail['y'],
                                                     pro_detail['w'], pro_detail['h'],
                                                     # pro_detail['cctv_id'], pro_detail['sort'],
                                                     pro_detail['cctv_id'],
                                                     userCd, pro_detail['title'])

            if result != None:
                add_arr.append(result)
                add_count += 1
        else:
            result = update_camera_monitoring_layout(pro_detail['profile_id'],
                                                     pro_detail['i'], pro_detail['x'],
                                                     pro_detail['y'], pro_detail['w'], pro_detail['h'],
                                                     pro_detail['cctv_id'],
                                                     userCd, pro_detail['title'])
            # pro_detail['sort'], userCd, pro_detail['title'])
            if result != None:
                update_arr.append([pro_detail['profile_id'], pro_detail['i']])
                update_count += 1


    if add_count != 0 or update_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "pro_detail added successfully", "add_count": add_count, "add_arr": add_arr,
            "update_count": update_count, "update_arr": update_arr}), 201
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "pro_detail added fail"}), 404


# 프로필 디테일 데이터 수정
@cctv_pro_detail.route('/pro_detail/<int:pro_detail_id>', methods=['PUT'])
def update_pro_detail(pro_detail_id):
    data = load_data(PRO_DETAIL_DATA_FILE)
    if str(pro_detail_id) not in data:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "pro_detail not found"}), 404
    pro_detail_data = request.json
    data[str(pro_detail_id)] = pro_detail_data
    print(pro_detail_data)
    save_data(data, PRO_DETAIL_DATA_FILE)
    return jsonify({"success": False,
                    "code": 404,
                    "msg": "pro_detail_data updated successfully"})

# 프르필 디테일 데이터 그룹으로 조회
@cctv_pro_detail.route('/group_pro_detail/<string:profile_id>', methods=['GET'])
def get_group_pro_detail(profile_id):
    # data = load_data(PRO_DETAIL_DATA_FILE)

    # # Transforming the input data to the desired output format1
    # # print(get_ratio("1"))
    # list = [
    #         {
    #             "id": key,
    #             "cctv_id": value["cctv_id"],
    #             "title": value["title"],
    #             "high": value["high"],
    #             "width": value["width"],
    #             "x": value["x"],
    #             "y": value["y"],
    #             "profile_id": value["profile_id"],
    #             "cctv_play_url": get_play_url(value["cctv_id"]),
    #             "ratio": get_ratio(value["cctv_id"])
    #             } for key, value in data.items()
    #               if value["profile_id"] == profile_id
    #         ]

    # output = {
    #     "list": list
    # }

    data = get_camera_monitoring_layout_by_monitoring_grp_id(profile_id)
    print('data', data)
    list = [
        {
            "i": value[1],
            "cctv_id": value[6],
            "title": value[12],
            "h": value[5],
            "w": value[4],
            "x": value[2],
            "y": value[3],
            "profile_id": value[0],
            "cctv_play_url": get_play_url(value[6]),
            "ratio": sanitize_ratio(get_ratio(value[6])),
            "created_at": serialize_datetime(value[8]),
            "created_by": value[9],
            "updated_at": serialize_datetime(value[10]),
            "updated_by": value[11]
        } for value in data
    ]

    output = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output)


# 프로필 디테일 데이터 조회 (특정 아이템 조회)
@cctv_pro_detail.route('/pro_detail/<string:pro_detail_id>', methods=['GET'])
def get_pro_detail(pro_detail_id):
    # data = load_data(PRO_DETAIL_DATA_FILE)
    # pro_detail = data.get(str(pro_detail_id))
    # if not pro_detail:
    #     return jsonify({"message": "pro_detail not found"}), 404

    # #형식 변경
    # output_data = {
    #     "data": pro_detail
    # }

    data = get_camera_monitoring_layout_by_id(pro_detail_id.split('_')[0], pro_detail_id.split('_')[1])

    pro_detail = {
                     "i": data[1],
                     "cctv_id": data[6],
                     "title": data[12],
                     "h": data[5],
                     "w": data[4],
                     "x": data[2],
                     "y": data[3],
                     "profile_id": data[0],
                     "cctv_play_url": get_play_url(data[6]),
                     "ratio": get_ratio(data[6]),
                     "created_at": data[8],
                     "created_by": data[9],
                     "updated_at": data[10],
                     "updated_by": data[11]
                 },

    # 형식 변경
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": pro_detail
    }

    return jsonify(output_data)


# 프로필 디테일 데이터 전체 조회
@cctv_pro_detail.route('/pro_details', methods=['GET'])
def get_all_pro_detail():
    # data = load_data(PRO_DETAIL_DATA_FILE)
    # #형식 변경
    # output_data = {
    #                 "list": [
    #                     {
    #                         "id": key,
    #                         "cctv_id": value["cctv_id"],
    #                         "title": value["title"],
    #                         "high": value["high"],
    #                         "profile_id": value["profile_id"],
    #                         "width": value["width"],
    #                         "x": value["x"],
    #                         "y": value["y"]
    #                     } for key, value in data.items()
    #                 ]
    # }
    layouts = get_all_camera_monitoring_layouts
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "i": data[1],
                "cctv_id": data[6],
                "title": data[12],
                "h": data[5],
                "w": data[4],
                "x": data[2],
                "y": data[3],
                "profile_id": data[0],
                "cctv_play_url": get_play_url(data[6]),
                "ratio": get_ratio(data[6]),
                "created_at": data[8],
                "created_by": data[9],
                "updated_at": data[10],
                "updated_by": data[11]
            } for data in layouts()
        ]
    }

    return jsonify(output_data)


# 프로필 디테일 데이터 삭제
@cctv_pro_detail.route('/pro_detail', methods=['DELETE'])
def delete_pro_detail():
    # data = load_data(PRO_DETAIL_DATA_FILE)
    # if str(pro_detail_id) not in data:
    #     return jsonify({"message": "pro_detail not found"}), 404
    # del data[str(pro_detail_id)]
    # save_data(data, PRO_DETAIL_DATA_FILE)
    pro_detail_data = request.json
    count = 0
    del_lst = []
    for pro_detail_id in pro_detail_data:
        print(pro_detail_id)
        result = delete_camera_monitoring_layout(pro_detail_id.split('_')[0], pro_detail_id.split('_')[1])
        if result != False:
            del_lst.append(pro_detail_id)
            count += 1
    if count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": f"pro_detail deleted successfully", "count": count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": f"pro_detail not found"})
