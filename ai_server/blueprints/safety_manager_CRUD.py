from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import get_all_managers, get_monitoring_grp, \
    get_group_info_by_comp_id, get_mt_manager, insert_safety_manager, update_safety_manager, return_monitoring_grp_id, \
    delete_manager, update_monitoring_grps, on_all_alarm, off_all_alarm

# 서버 정보
# 블루프린트 생성, /cctv/server_crud 상위 경로 추가
manager_crud = Blueprint('manager_crud', __name__, url_prefix='/cctv/manager_crud')


###READ###
# cctv 데이터 전체 조회



@manager_crud.route('/lists', methods=['GET'])
def get_all_manager_list():
    # data = load_data(CCTV_DATA_FILE)
    # Transforming the input data to the desired output format
    data = get_all_managers()
    list = [
        {
            "monitoring_grp_id": manager[0],
            "chat_id": manager[1],
            "notification_on": manager[2],
            "created_at": manager[3].strftime("%Y.%m.%d %H:%M:%S") if manager[3] is not None else None,
            "created_by": manager[4],
            "updated_at": manager[5].strftime("%Y.%m.%d %H:%M:%S") if manager[5] is not None else None,
            "updated_by": manager[6],       
            "token": manager[8],
        } for manager in data
    ]
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }
    return jsonify(output_data)


# 안전 관리자 데이터 전체 조회
@manager_crud.route('/managers/<string:userCd>', methods=['GET'])
def get_all_managers1(userCd):

    parts = userCd.split('_', 1)

    if parts[0] == 'IGNS':
        data = get_all_managers()
    else:
        data = get_group_info_by_comp_id(parts[0])

    # Transforming the input data to the desired output format
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "comp_id": manager[0],
                "monitoring_grp_id": manager[1],
                "grp_nm": manager[2],
                "created_at": manager[3].strftime("%Y.%m.%d %H:%M:%S") if manager[3] is not None else None,
                "created_by": manager[4],
                "updated_at": manager[5].strftime("%Y.%m.%d %H:%M:%S") if manager[5] is not None else None,
                "updated_by": manager[6],
            } for manager in data
        ]
    }
    return jsonify(output_data)


# monitoring_grp_id에 해당하는 안전 관리자 데이터 조회
@manager_crud.route('/manager/<string:manager_id>', methods=['GET'])
def get_manager(manager_id):
    manager = get_mt_manager(manager_id)

    if not manager:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "Server not found"}), 404
    # Transforming the input data to the desired output format
    manager = {
        "monitoring_grp_id": manager[0],
        "chat_id": manager[1],
        "modified_at": manager[3].strftime("%Y.%m.%d %H:%M:%S") if manager[3] is not None else None,
        "notification_on": manager[4],
    }
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": manager
    }
    return jsonify(output_data)


###CREATE && UPDATE###
@manager_crud.route('/manager_test', methods=['POST'])
def test_add_manager():
    try:
        # ✅ Expecting JSON array (list of dictionaries)
        data_list = request.get_json()

        if not data_list:
            return jsonify({"success": False, "message": "No JSON data received"}), 400

        if not isinstance(data_list, list):
            return jsonify({"success": False, "message": "Expected a JSON array"}), 400

        insert_results = []
        update_results = []

        for data in data_list:
            monitoring_grp_id = data.get("monitoring_grp_id")
            chat_id = data.get("chat_id")
            notification_on = data.get("notification_on")
            created_by = data.get("created_by")
            comp_id = data.get("comp_id")
            update_flag = data.get("update", False)  # ✅ Flag to check if it's an update
            original_monitoring_grp_id = data.get("original_monitoring_grp_id")
            original_chat_id = data.get("original_chat_id")

            # ✅ Validate required fields
            if not monitoring_grp_id:
                return jsonify({"success": False, "message": "monitoring_grp_id is required"}), 400

            # ✅ Ensure chat_id is an integer (if provided)
            if chat_id is not None:
                try:
                    chat_id = int(chat_id)
                except ValueError:
                    return jsonify({"success": False, "message": "chat_id must be an integer"}), 400

            # ✅ Check if this is an UPDATE or INSERT operation
            if update_flag and original_monitoring_grp_id and original_chat_id:
                # ✅ Perform update
                updated = update_safety_manager(
                    monitoring_grp_id=monitoring_grp_id,
                    chat_id=chat_id,
                    notification_on=notification_on,
                    updated_by=created_by,
                    original_monitoring_grp_id=original_monitoring_grp_id,
                    original_chat_id=original_chat_id
                )
                if updated:
                    update_results.append({
                        "monitoring_grp_id": monitoring_grp_id,
                        "chat_id": chat_id,
                        "success": True,
                        "message": "Update successful"
                    })
                else:
                    update_results.append({
                        "monitoring_grp_id": monitoring_grp_id,
                        "chat_id": chat_id,
                        "success": False,
                        "message": "Update failed"
                    })
            else:
                # ✅ Perform insert
                result = insert_safety_manager(
                    monitoring_grp_id=monitoring_grp_id,
                    chat_id=chat_id,
                    notification_on=notification_on,
                    created_by=created_by,
                    comp_id=comp_id
                )
                if result:
                    insert_results.append({
                        "monitoring_grp_id": result[0],
                        "chat_id": result[1],
                        "success": True,
                        "message": "Insert successful"
                    })
                else:
                    insert_results.append({
                        "monitoring_grp_id": monitoring_grp_id,
                        "chat_id": chat_id,
                        "success": False,
                        "message": "Insert failed"
                    })

        return jsonify({
            "success": True,
            "insert_results": insert_results,
            "update_results": update_results
        })

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500




# manager_list 데이터 추가
@manager_crud.route('/manager', methods=['POST'])
def add_manager():
    try:
        # ✅ Expecting a JSON array
        cctv_data = request.get_json()

        if not cctv_data:
            return jsonify({"success": False, "message": "No data received"}), 400

        if not isinstance(cctv_data, list):
            return jsonify({"success": False, "message": "Expected a JSON array"}), 400

        insert_results = []
        update_results = []

        for cctv in cctv_data:
            try:
                # ✅ Extracting common fields
                __created__ = cctv.get("__created__", False)
                userCd = cctv.get("userCd", "").strip()
                monitoring_grp_id = cctv.get("monitoring_grp_id", "").strip()
                chat_id = cctv.get("chat_id", "").strip()
                token = cctv.get("token", "").strip()
                notification_on = cctv.get("notification_on", False)

                # ✅ Ensure userCd has a valid format
                if "_" in userCd:
                    comp_id, userCd = userCd.split('_', 1)
                else:
                    return jsonify({"success": False, "message": f"Invalid userCd format: {userCd}"}), 400

                # ✅ Validate chat_id
                if chat_id in ["", "{chat_id}"]:
                    chat_id = None  # Set to None if empty placeholder

                # ✅ Ensure boolean conversion for notification_on
                if isinstance(notification_on, str):
                    notification_on = notification_on.lower() in ["true", "1", "t", "yes"]

                if __created__:
                    # ✅ Insert new manager
                    result = insert_safety_manager(
                        monitoring_grp_id, chat_id, notification_on, userCd, comp_id, token
                    )
                    if result:
                        insert_results.append({
                            "monitoring_grp_id": result[0],
                            "chat_id": result[1],
                            "token":  result[5],
                            "success": True,
                            "message": "Insert successful"
                        })
                    else:
                        insert_results.append({
                            "monitoring_grp_id": monitoring_grp_id,
                            "chat_id": chat_id,
                            "token":  token,
                            "success": False,
                            "message": "Insert failed"
                        })
                else:
                    # ✅ Update existing manager
                    original_monitoring_grp_id = cctv.get("original_monitoring_grp_id")
                    original_chat_id = cctv.get("original_chat_id")

                    if not original_monitoring_grp_id or not original_chat_id:
                        return jsonify({
                            "success": False,
                            "message": "Missing original_monitoring_grp_id or original_chat_id for update"
                        }), 400

                    updated = update_safety_manager(
                        monitoring_grp_id=monitoring_grp_id,
                        chat_id=chat_id,
                        token=token,
                        notification_on=notification_on,
                        updated_by=userCd,
                        original_monitoring_grp_id=original_monitoring_grp_id,
                        original_chat_id=original_chat_id
                    )

                    if updated:
                        update_results.append({
                            "monitoring_grp_id": monitoring_grp_id,
                            "chat_id": chat_id,
                            "token": token,
                            "success": True,
                            "message": "Update successful"
                        })
                    else:
                        update_results.append({
                            "monitoring_grp_id": monitoring_grp_id,
                            "chat_id": chat_id,
                            "token": token,
                            "success": False,
                            "message": "Update failed"
                        })

            except Exception as e:
                return jsonify({"success": False, "message": str(e)}), 500

        return jsonify({
            "success": True,
            "insert_results": insert_results,
            "update_results": update_results
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Server error: {str(e)}"}), 500





###DELETE###
# cctv 데이터 삭제
@manager_crud.route('/manager', methods=['DELETE'])
def delete_cctv():
    cctv_data = request.json  # Expecting a list of dicts

    # Ensure that the request contains a list
    if not isinstance(cctv_data, list):
        return jsonify({"success": False, "code": 400, "msg": "Invalid request format"}), 400

    count = 0
    del_lst = []

    for item in cctv_data:
        # Ensure each item is a dictionary
        if not isinstance(item, dict):
            return jsonify({"success": False, "code": 400, "msg": "Invalid data format, expected list of objects"}), 400

        monitoring_grp_id = item.get("monitoring_grp_id")
        chat_id = item.get("chat_id")

        if not monitoring_grp_id or not chat_id:
            return jsonify({"success": False, "code": 400, "msg": "Missing parameters"}), 400

        result = delete_manager(monitoring_grp_id, chat_id)

        if result:
            del_lst.append({"monitoring_grp_id": monitoring_grp_id, "chat_id": chat_id})
            count += 1

    if count > 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "cctv deleted successfully",
            "count": count,
            "del_lst": del_lst
        })
    else:
        return jsonify({"success": False, "code": 404, "msg": "cctv not found"}), 404



###SET_ALARM###
# cctv 전체 실행
@manager_crud.route('/run_all', methods=['GET'])
def run_all():
    # Load all managers from the database
    cctv_data = get_all_managers()
    print("Retrieved CCTV Data:", cctv_data)  # Debugging step

    count = 0  # Counter for successful updates

    for cctv in cctv_data:
        print("Processing Row:", cctv)  # Debugging step

        # Ensure the record has at least 3 values
        if len(cctv) < 3:
            print(f"Skipping invalid record: {cctv}")
            continue  # Skip malformed records

        # Unpack only the required fields
        monitoring_grp_id, chat_id, notification_on = cctv[:3]  # Take only the first three values

        if not notification_on:  # Only update if notification is currently off
            success = on_all_alarm(monitoring_grp_id, chat_id)

            if success:
                count += 1

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": "Updated successfully",
        "updated_count": count
    })




@manager_crud.route('/run_alarm/<string:composite_key>', methods=['GET'])
def run_alarm(composite_key):
    try:
        monitoring_grp_id, chat_id = composite_key.split("_")  # Example format: "CMG0002-67890"
    except ValueError:
        return jsonify({"success": False, "code": 400, "msg": "Invalid key format"}), 400

    success = on_all_alarm(monitoring_grp_id, chat_id)

    if success:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "Alarm turned on successfully"
        })
    else:
        return jsonify({"success": False, "code": 500, "msg": "Failed to update alarm status"}), 500


@manager_crud.route('/stop_alarm/<string:composite_key>', methods=['GET'])
def stop_alarm(composite_key):  # ✅ Renamed function to avoid conflict
    try:
        monitoring_grp_id, chat_id = composite_key.split("_")  # Example format: "CMG0002-67890"
    except ValueError:
        return jsonify({"success": False, "code": 400, "msg": "Invalid key format"}), 400

    success = off_all_alarm(monitoring_grp_id, chat_id)

    if success:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "Alarm turned off successfully"
        })
    else:
        return jsonify({"success": False, "code": 500, "msg": "Failed to update alarm status"}), 500


