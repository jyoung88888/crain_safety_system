from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import insert_camera_monitoring_grp, update_camera_monitoring_grp, \
    get_camera_monitoring_grp_by_id, get_all_camera_monitoring_grps, get_camera_monitoring_grp_by_comp_id, \
    delete_camera_monitoring_grp

# 모니터링 관련 data
# 블루프린트 생성, /cctv/profile_crud 상위 경로 추가
cctv_profile = Blueprint('cctv_profile_crud', __name__, url_prefix='/cctv/profile_crud')

# 프로필 데이터 추가
@cctv_profile.route('/profile', methods=['POST'])
def profile():
    # data = load_data(PROFILE_DATA_FILE)
    # new_id = int(max(data.keys(), default=0)) + 1
    # profile_data = request.json
    # data[new_id] = profile_data
    # save_data(data, PROFILE_DATA_FILE)
    profile_data = request.json

    add_count = 0
    add_arr = []
    update_count = 0
    update_arr = []

    for profile in profile_data:
        try:
            __created__ = profile["__created__"]
        except Exception as e:
            __created__ = False

        try:
            comp_id = profile['comp_id']
        except Exception as e:
            parts = profile['userCd'].split('_', 1)
            # comp_id = get_comp_id_by_user_cd(server['userCd'])
            comp_id = parts[0]
            userCd = parts[1]

        if __created__:
            result = insert_camera_monitoring_grp(comp_id, profile["Profile_name"],
                                                  userCd)

            if result != None:
                add_arr.append(result)
                add_count += 1
        else:
            result = update_camera_monitoring_grp(profile["Profile_id"], profile["Profile_name"],
                                                  userCd)

            if result != False:
                update_arr.append(profile['Profile_id'])
                update_count += 1

    if add_count != 0 or update_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "profile added successfully",
            "add_count": add_count, "add_arr": add_arr,
            "update_count": update_count, "update_arr": update_arr}), 201
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "profile added fail"}), 404


# 프로필 데이터 수정
@cctv_profile.route('/profile/<string:profile_id>', methods=['PUT'])
def update_profile(profile_id):
    # data = load_data(PROFILE_DATA_FILE)
    # if str(profile_id) not in data:
    #     return jsonify({"message": "profile not found"}), 404
    # profile_data = request.json
    # data[str(profile_id)] = profile_data
    # print(profile_data)
    # save_data(data, PROFILE_DATA_FILE)
    profile_data = request.json
    result = update_camera_monitoring_grp(profile_id, profile_data["Profile_name"],
                                          profile_data["updated_by"])
    if result != False:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "profile updated successfully"})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "profile not found"}), 404


# 프로필 데이터 조회 (특정 아이템 조회)
@cctv_profile.route('/profile/<string:profile_id>', methods=['GET'])
def get_profile(profile_id):
    # data = load_data(PROFILE_DATA_FILE)
    # profile = data.get(str(profile_id))
    # if not profile:
    #     return jsonify({"message": "profile not found"}), 404

    # #형식 변경
    # output_data = {
    #     "data": profile
    # }
    profile = get_camera_monitoring_grp_by_id(profile_id)
    if not profile:
        return jsonify({"message": "profile not found"}), 404
    # Transforming the input data to the desired output format
    profile = {
        "comp_id": profile[0],
        "Profile_id": profile[1],
        "Profile_name": profile[2],
        "created_at": profile[3].strftime("%Y.%m.%d %H:%M:%S") if profile[3] != None else None,
        # "created_at": profile[3],
        "created_by": profile[4],
        # "updated_at": profile[5],
        "updated_at": profile[5].strftime("%Y.%m.%d %H:%M:%S") if profile[5] != None else None,
        "updated_by": profile[6],
    }

    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": profile
    }
    return jsonify(output_data)


# 프로필 데이터 전체 조회
@cctv_profile.route('/profiles', methods=['GET'])
def get_all_profiles():
    # data = load_data(PROFILE_DATA_FILE)
    # #형식 변경
    # output_data = {
    #     "list": [
    #         {
    #             "id": key,
    #             "Profile_name": value["Profile_name"],
    #         } for key, value in data.items()
    #     ]
    # }

    data = get_all_camera_monitoring_grps()
    list = [
        {
            "comp_id": profile[0],
            "Profile_id": profile[1],
            "Profile_name": profile[2],
            "created_at": profile[3].strftime("%Y.%m.%d %H:%M:%S") if profile[3] != None else None,
            # "created_at": profile[3],
            "created_by": profile[4],
            # "updated_at": profile[5],
            "updated_at": profile[5].strftime("%Y.%m.%d %H:%M:%S") if profile[5] != None else None,
            "updated_by": profile[6]
        } for profile in data
    ]
    list = sorted(list, key=lambda x: x['Profile_id'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# 프로필 데이터 전체 조회
@cctv_profile.route('/profiles/<string:userCd>', methods=['GET'])
def get_all_profiles1(userCd):
    # data = load_data(PROFILE_DATA_FILE)
    # #형식 변경
    # output_data = {
    #     "list": [
    #         {
    #             "id": key,
    #             "Profile_name": value["Profile_name"],
    #         } for key, value in data.items()
    #     ]
    # }

    parts = userCd.split('_', 1)

    if parts[0] == 'IGNS':
        data = get_all_camera_monitoring_grps()
    else:
        data = get_camera_monitoring_grp_by_comp_id(parts[0])

    list = [
        {
            "comp_id": profile[0],
            "Profile_id": profile[1],
            "Profile_name": profile[2],
            "created_at": profile[3].strftime("%Y.%m.%d %H:%M:%S") if profile[3] != None else None,
            # "created_at": profile[3],
            "created_by": profile[4],
            # "updated_at": profile[5],
            "updated_at": profile[5].strftime("%Y.%m.%d %H:%M:%S") if profile[5] != None else None,
            "updated_by": profile[6]
        } for profile in data
    ]
    list = sorted(list, key=lambda x: x['Profile_id'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# 프로필 데이터 삭제
@cctv_profile.route('/profile', methods=['DELETE'])
def delete_profile():
    # data = load_data(PROFILE_DATA_FILE)
    # if str(profile_id) not in data:
    #     return jsonify({"message": "profile not found"}), 404
    # del data[str(profile_id)]
    # save_data(data, PROFILE_DATA_FILE)
    profile_data = request.json
    count = 0
    del_lst = []
    for profile_id in profile_data:
        result = delete_camera_monitoring_grp(profile_id)
        if result != False:
            del_lst.append(profile_id)
            count += 1
    if count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": f"profile  deleted successfully", "count": count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "profile not found"})
