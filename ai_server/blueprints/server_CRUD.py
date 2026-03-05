from flask import Blueprint, request, jsonify

from blueprints.lib.public_func import insert_ai_server, update_ai_server, get_ai_server, get_ai_server_all, \
    get_ai_server_by_comp_id, delete_ai_server

# 서버 정보
# 블루프린트 생성, /cctv/server_crud 상위 경로 추가
cctv_server = Blueprint('cctv_server', __name__, url_prefix='/cctv/server_crud')

# 서버 데이터 추가
@cctv_server.route('/server', methods=['POST'])
def add_server():

    server_data = request.json
    print(server_data)
    add_count = 0
    add_arr = []
    update_count = 0
    update_arr = []
    for server in server_data:
        try:
            __created__ = server["__created__"]
        except Exception as e:
            __created__ = False

        try:
            comp_id = server['comp_id']
            parts = server['userCd'].split('_', 1)
            userCd = parts[1]
        except Exception as e:
            parts = server['userCd'].split('_', 1)
            # comp_id = get_comp_id_by_user_cd(server['userCd'])
            comp_id = parts[0]
            userCd = parts[1]

        print('comp_id: ', comp_id)

        if __created__:
            result = insert_ai_server(comp_id, server['server_name'],
                                      server['server_ip'], server['restapi_port'],
                                      server['mediamtx_port'], server['remark'],
                                      userCd)
            if result != None:
                add_arr.append(result)
                add_count += 1
        else:
            result = update_ai_server(server['server_id'],
                                      comp_id, server['server_name'],
                                      server['server_ip'], server['restapi_port'],
                                      server['mediamtx_port'], server['remark'],
                                      userCd)
            if result != None:
                update_arr.append(server['server_id'])
                update_count += 1


    if add_count != 0 or update_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "add_count": add_count, "add_arr": add_arr,
            "update_count": update_count, "update_arr": update_arr
        })
    else:
        return jsonify({
            "success": False,
            "code": 404,
            "msg": "오류발생."
        })


# 서버 데이터 수정
@cctv_server.route('/server/<string:server_id>', methods=['PUT'])
def update_server(server_id):

    server_data = request.json

    result = update_ai_server(server_id, server_data['comp_id'], server_data['server_name'],
                              server_data['server_ip'], server_data['restapi_port'],
                              server_data['mediamtx_port'], server_data['remark'],
                              server_data['userCd'])

    if result != False:
        return jsonify({"success": True,
                        "code": 200,
                        "msg": "성공하였습니다.", })
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "Server not found"})


# 서버 데이터 조회 (특정 아이템 조회)
@cctv_server.route('/server/<string:server_id>', methods=['GET'])
def get_server(server_id):

    server = get_ai_server(server_id)
    if not server:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "Server not found"}), 404
    # Transforming the input data to the desired output format
    server = {
        "comp_id": server[0],
        "server_id": server[1],
        "server_name": server[2],
        "server_ip": server[3],
        "restapi_port": server[4],
        "mediamtx_port": server[5],
        "remark": server[6],
        # "created_t":  server[7],
        "created_at": server[7].strftime("%Y.%m.%d %H:%M:%S") if server[7] != None else None,
        "created_by": server[8],
        # "updated_at": server[9],
        "updated_at": server[9].strftime("%Y.%m.%d %H:%M:%S") if server[9] != None else None,
        "updated_by": server[10]
    }
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": server
    }
    return jsonify(output_data)


# 서버 데이터 전체 조회
@cctv_server.route('/servers', methods=['GET'])
def get_all_servers():
    data = get_ai_server_all()
    # Transforming the input data to the desired output format
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "server_id": server[1],
                "comp_id": server[0],
                "created_at": server[7],
                "created_by": "adim",
                "mediamtx_port": server[5],
                "remark": server[6],
                "restapi_port": server[4],
                "server_ip": server[3],
                "server_name": server[2],
                # "created_at":  server[7],
                "created_at": server[7].strftime("%Y.%m.%d %H:%M:%S") if server[7] != None else None,
                "created_by": server[8],
                # "updated_at": server[9],
                "updated_at": server[9].strftime("%Y.%m.%d %H:%M:%S") if server[9] != None else None,
                "updated_by": server[10]
            } for server in data
        ]
    }
    return jsonify(output_data)


# 서버 데이터 전체 조회
@cctv_server.route('/servers/<string:userCd>', methods=['GET'])
def get_all_servers1(userCd):

    parts = userCd.split('_', 1)

    if parts[0] == 'IGNS':
        data = get_ai_server_all()
    else:
        data = get_ai_server_by_comp_id(parts[0])

    # Transforming the input data to the desired output format
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "server_id": server[1],
                "comp_id": server[0],
                "created_at": server[7],
                "created_by": "adim",
                "mediamtx_port": server[5],
                "remark": server[6],
                "restapi_port": server[4],
                "server_ip": server[3],
                "server_name": server[2],
                # "created_at":  server[7],
                "created_at": server[7].strftime("%Y.%m.%d %H:%M:%S") if server[7] != None else None,
                "created_by": server[8],
                # "updated_at": server[9],
                "updated_at": server[9].strftime("%Y.%m.%d %H:%M:%S") if server[9] != None else None,
                "updated_by": server[10]
            } for server in data
        ]
    }
    return jsonify(output_data)


# 서버 데이터 삭제
@cctv_server.route('/server', methods=['DELETE'])
def delete_server():

    server_data = request.json
    ok_count = 0
    fail_count = 0
    del_lst = []
    for server_id in server_data:
        result = delete_ai_server(server_id)
        if result:
            ok_count += 1
            del_lst.append(server_id)
        else:
            fail_count += 1

    if ok_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "count": ok_count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "Server not found"})
