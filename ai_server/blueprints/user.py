from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import load_data, USER_DATA_FILE, encrypt_password, save_data, verify_password

# 사용자 정보
# 블루프린트 생성, /cctv/user 상위 경로 추가
cctv_user = Blueprint('cctv_user_crud', __name__, url_prefix='/cctv/user_crud')

# 사용자 데이터 추가
@cctv_user.route('/user', methods=['POST'])
def user():
    data = load_data(USER_DATA_FILE)
    new_id = int(max(data.keys(), default=0)) + 1
    user_data = request.json
    # 패스워드 암호화
    user_data['user_pw'] = encrypt_password(user_data['user_pw'])
    data[new_id] = user_data
    save_data(data, USER_DATA_FILE)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": "user added successfully", "id": new_id}), 201


# 사용자 데이터 수정
@cctv_user.route('/user/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = load_data(USER_DATA_FILE)
    if str(user_id) not in data:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "user not found"})
    user_data = request.json
    # 패스워드 암호화
    user_data['user_pw'] = encrypt_password(user_data['user_pw'])
    # print(encrypt_password(user_data['user_pw']))
    data[str(user_id)] = user_data
    print(user_data)
    save_data(data, USER_DATA_FILE)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": "user updated successfully"})


# 사용자 데이터 조회 (특정 아이템 조회)
@cctv_user.route('/user/<int:user_id>', methods=['GET'])
def get_user(user_id):
    data = load_data(USER_DATA_FILE)
    user = data.get(str(user_id))
    if not user:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "user not found"})
    # Transforming the input data to the desired output format
    output_data = {
        "data": user
    }

    return jsonify(output_data)


# 사용자 데이터 전체 조회
@cctv_user.route('/users', methods=['GET'])
def get_all_users():
    data = load_data(USER_DATA_FILE)
    # Transforming the input data to the desired output format
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "id": key,
                "admin_chk": value["admin_chk"],
                "user_id": value["user_id"],
                "user_pw": value["user_pw"]
            } for key, value in data.items()
        ]
    }
    return jsonify(output_data)


# 사용자 데이터 삭제
@cctv_user.route('/user/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    data = load_data(USER_DATA_FILE)
    if str(user_id) not in data:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "user not found"})
    del data[str(user_id)]
    save_data(data, USER_DATA_FILE)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "message": f"user {user_id} deleted successfully"})


# 로그인
@cctv_user.route('/login', methods=['POST'])
def login():
    data = load_data(USER_DATA_FILE)
    print(request.json)

    for user in data:
        if data[user]['user_id'] == request.json['user_id']:
            print(data[user]['user_pw'])
            print(request.json['user_pw'])
            if verify_password(request.json['user_pw'], data[user]['user_pw']):
                return jsonify({
                    "success": True,
                    "code": 200,
                    "msg": "성공하였습니다.",
                    "message": f"Allow access"})
            break

    return jsonify({"success": False,
                    "code": 404,
                    "msg": f"Inaccessible"})

