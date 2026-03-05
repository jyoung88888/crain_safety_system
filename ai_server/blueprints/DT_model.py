import os

import requests
from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import select_dt_Modeling_data, \
    select_detected_object_dimensions, select_detected_object_inventories, \
    insert_user_input_data, get_next_input_id, get_new_image_id, \
    select_user_input_data, updateUserInputData, DeleteUserInputData, \
    return_selected_object_imgs, select_collision_events, get_sim_progress_bwts, \
    select_safety_detected_object_dimensions, select_dt_safety_Modeling_data, \
    select_detected_object_inventories_hoseon, fn_add_twin_detection_filter, \
    fn_delete_twin_detection_filter, fn_list_twin_detection_filters, \
    detected_objects, get_filter_label_counts, _norm, subtract_label_counts, \
    get_sim_progress_scrubber
from lib.db import get_connection


# 모델 마스터
# 블루프린트 생성, /cctv/model_crud 상위 경로 추가
dt_model = Blueprint('dt_model', __name__, url_prefix='/cctv/dt_model')

# ==========================
# 🔹 REST API 엔드포인트
# ==========================
@dt_model.route('/twin-detection-filter/detected-objects/<int:filter_id>', methods=['GET'])
def api_select_detected_object_dimensions(filter_id: int):
    """
    GET /twin-detection-filter/detected-objects/<filter_id>

    예)
      GET /twin-detection-filter/detected-objects/10

    응답 예:
    {
      "status": "success",
      "data": [
        {"image_id": 123, "detection_id": 10},
        {"image_id": 123, "detection_id": 11}
      ]
    }
    """
    result = detected_objects(filter_id)

    # status에 따라 HTTP 코드 나눠주기 (선택사항)
    if result.get("status") == "success":
        return jsonify(result), 200

    # 에러 메시지에 따라 404 / 500 정도로 분기
    msg = result.get("message", "")
    if "not found" in msg or "No image found" in msg or "No detection matched" in msg:
        return jsonify(result), 404

    return jsonify(result), 500

# 오탐list 조회
@dt_model.route('/twin-detection-filter', methods=['GET'])
def list_twin_detection_filters():
    """
    GET /twin-detection-filter?camera_id=CAM0001&object_label=BWTS&limit=50&offset=0
    - 파라미터는 선택(없으면 전체)
    """
    camera_id    = request.args.get("camera_id", None)
    object_label = request.args.get("object_label", None)

    try:
        limit  = int(request.args.get("limit", 100))
        offset = int(request.args.get("offset", 0))
        limit  = 1 if limit < 1 else (200 if limit > 200 else limit)
        offset = 0 if offset < 0 else offset
    except ValueError:
        return jsonify({"error": "limit/offset는 정수여야 합니다."}), 400

    result, err = fn_list_twin_detection_filters(camera_id, object_label, limit, offset)
    if err:
        return jsonify({"error": err}), 500
    return jsonify(result), 200

# 오탐 삭제
@dt_model.route('/twin-detection-filter/<int:filter_id>', methods=['DELETE'])
def delete_twin_detection_filter(filter_id: int):
    """
    DELETE /twin-detection-filter/{filter_id}
    """
    row, err = fn_delete_twin_detection_filter(filter_id)
    if err:
        return jsonify({"error": err}), 500
    if not row:
        return jsonify({"error": f"filter_id={filter_id} 데이터가 없습니다."}), 404
    return jsonify(row), 200

# 오탐 내역 추가
@dt_model.route('/twin-detection-filter', methods=['POST'])
def add_twin_detection_filter():
    """
    POST /twin-detection-filter
    Body(JSON):
    {
      "camera_id": "CAM0001",
      "grid_width": 3,
      "grid_height": 3,
      "detected_row": 1,
      "detected_col": 2,
      "object_label": "BWTS"
    }
    """
    try:
        body = request.get_json(force=True) or {}
        camera_id    = (body.get("camera_id") or "").strip()
        object_label = (body.get("object_label") or "").strip()

        # 정수 변환 및 검증
        try:
            grid_width   = int(body.get("grid_width"))
            grid_height  = int(body.get("grid_height"))
            detected_row = int(body.get("detected_row"))
            detected_col = int(body.get("detected_col"))
        except (TypeError, ValueError):
            return jsonify({"error": "grid_width, grid_height, detected_row, detected_col는 정수여야 합니다."}), 400

        if not camera_id or not object_label:
            return jsonify({"error": "camera_id, object_label는 필수입니다."}), 400
        if grid_width <= 0 or grid_height <= 0:
            return jsonify({"error": "grid_width, grid_height는 1 이상이어야 합니다."}), 400
        # 행/열 인덱스는 0 또는 1부터 운영 정책 따라 다름 → 0 이상만 강제
        if detected_row < 0 or detected_col < 0:
            return jsonify({"error": "detected_row, detected_col는 0 이상이어야 합니다."}), 400

        result, err = fn_add_twin_detection_filter(
            camera_id, grid_width, grid_height, detected_row, detected_col, object_label
        )
        if err:
            return jsonify({"error": err}), 500

        status = 201 if result["created"] else 200
        return jsonify(result), status

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@dt_model.route('/dt_safety_grid_info', methods=['GET'])
def dt_safety_grid_info():
    """DT 모델링 데이터를 반환하는 API"""
    response = select_dt_safety_Modeling_data()
    return jsonify(response)

@dt_model.route('/dt_grid_info', methods=['GET'])
def dt_grid_info():
    """DT 모델링 데이터를 반환하는 API"""
    response = select_dt_Modeling_data()
    return jsonify(response)

@dt_model.route('/dt_modeling_bwts/<camera_id>', methods=['GET'])
def dt_modeling_data_api_bwts(camera_id):
    """
    시뮬레이션의 진행률을 계산하여 반환합니다.
    mac_name이 중복일 경우 하나만 반환합니다.
    mac_name이 null, None 또는 'none'(대소문자 무관)인 경우 반환하지 않습니다.
    mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출하여 반환합니다.
    camera_id에 해당하는 탐지된 객체 정보도 함께 반환합니다.
    sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.

    Parameters:
        camera_id (str): 필수 - 특정 카메라 ID로 필터링 (URL 경로에서 추출)
        base_time (str): 필수 - 기준 날짜 (YYYY-MM-DD HH:MM:SS 형식)
        sim_id (int, optional): 선택 - 시뮬레이션 ID. 제공되지 않으면 select_detected_object_dimensions 함수를 사용

    Returns:
        JSON: 진행률 정보 (progress_percentage, sim_id, mac_id, mac_name, start_date, end_date)
              mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환되며, camera_id, row_coord, col_coord, width, height 값도 포함됩니다.
              camera_id는 mac_name에서 추출한 카메라 ID(예: CAM0063)입니다.
              camera_id에 해당하는 탐지된 객체 정보도 detected_objects 키에 포함됩니다.
              sim_id가 제공되지 않은 경우 select_detected_object_dimensions 함수의 결과가 반환됩니다.
              mac_name이 중복일 경우 하나만 반환합니다.
              mac_name이 null, None 또는 'none'(대소문자 무관)인 항목은 반환되지 않습니다.
              sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.
    """

    print('dt_modeling_data_api_bwts run') 

    if not camera_id:
        return jsonify({"status": "error", "message": "Missing required parameter: camera_id"}), 400
    
    print('camera_id', camera_id)
    sim_id = request.args.get('sim_id')
    print('sim_id', sim_id)
    base_time = request.args.get('base_time')
    print('base_time', base_time)

    if not base_time:
        return jsonify({
            "status": "error",
            "message": "base_time 파라미터가 필요합니다."
        }), 400

    # sim_id가 None일 때는 select_detected_object_dimensions 함수 사용
    if not sim_id:
        result = select_detected_object_dimensions(camera_id, base_time=base_time)

        # 에러 처리
        if isinstance(result, dict) and result["status"] != "success":
            return jsonify({
                "status": "error",
                "message": result.get("message", "Unknown error")
            }), 404

        extracted_data = result["data"]
    else:
        # sim_id가 있을 때는 get_sim_progress 함수 사용
        result = get_sim_progress_bwts(sim_id, base_time, camera_id)

        # 에러 처리
        if isinstance(result, dict) and 'error' in result:
            return jsonify({
                "status": "error",
                "message": result['error']
            }), 404

        # 결과가 리스트가 아니면 리스트로 변환
        if not isinstance(result, list):
            result = [result]

        # 결과가 단일 항목만 있으면 첫 번째 항목만 반환
        if len(result) == 1:
            result = result[0]

    return jsonify({
        "status": "success",
        "data": result
    })


@dt_model.route('/dt_modeling_scrubber/<camera_id>', methods=['GET'])
def dt_modeling_data_api_scrubber(camera_id):
    """
    시뮬레이션의 진행률을 계산하여 반환합니다.
    mac_name이 중복일 경우 하나만 반환합니다.
    mac_name이 null, None 또는 'none'(대소문자 무관)인 경우 반환하지 않습니다.
    mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환하며, camera_id, row_coord, col_coord, width, height 값을 추출하여 반환합니다.
    camera_id에 해당하는 탐지된 객체 정보도 함께 반환합니다.
    sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.

    Parameters:
        camera_id (str): 필수 - 특정 카메라 ID로 필터링 (URL 경로에서 추출)
        base_time (str): 필수 - 기준 날짜 (YYYY-MM-DD HH:MM:SS 형식)
        sim_id (int, optional): 선택 - 시뮬레이션 ID. 제공되지 않으면 select_detected_object_dimensions 함수를 사용

    Returns:
        JSON: 진행률 정보 (progress_percentage, sim_id, mac_id, mac_name, start_date, end_date)
              mac_name이 CAM0063_R2C0_10x8 형식일 경우에만 반환되며, camera_id, row_coord, col_coord, width, height 값도 포함됩니다.
              camera_id는 mac_name에서 추출한 카메라 ID(예: CAM0063)입니다.
              camera_id에 해당하는 탐지된 객체 정보도 detected_objects 키에 포함됩니다.
              sim_id가 제공되지 않은 경우 select_detected_object_dimensions 함수의 결과가 반환됩니다.
              mac_name이 중복일 경우 하나만 반환합니다.
              mac_name이 null, None 또는 'none'(대소문자 무관)인 항목은 반환되지 않습니다.
              sim_id가 있을 때 detected_col, detected_row, grid_height, grid_width, input_col, input_row의 값을 각각 sim_col_coord, sim_row_coord, sim_height, sim_width의 값으로 대체합니다.
    """

    print('dt_modeling_data_api_bwts run') 

    if not camera_id:
        return jsonify({"status": "error", "message": "Missing required parameter: camera_id"}), 400
    
    print('camera_id', camera_id)
    sim_id = request.args.get('sim_id')
    print('sim_id', sim_id)
    base_time = request.args.get('base_time')
    print('base_time', base_time)

    if not base_time:
        return jsonify({
            "status": "error",
            "message": "base_time 파라미터가 필요합니다."
        }), 400

    # sim_id가 None일 때는 select_detected_object_dimensions 함수 사용
    if not sim_id:
        result = select_detected_object_dimensions(camera_id, base_time=base_time)

        # 에러 처리
        if isinstance(result, dict) and result["status"] != "success":
            return jsonify({
                "status": "error",
                "message": result.get("message", "Unknown error")
            }), 404

        extracted_data = result["data"]
    else:
        # sim_id가 있을 때는 get_sim_progress 함수 사용
        result = get_sim_progress_scrubber(sim_id, base_time, camera_id)

        # 에러 처리
        if isinstance(result, dict) and 'error' in result:
            return jsonify({
                "status": "error",
                "message": result['error']
            }), 404

        # 결과가 리스트가 아니면 리스트로 변환
        if not isinstance(result, list):
            result = [result]

        # 결과가 단일 항목만 있으면 첫 번째 항목만 반환
        if len(result) == 1:
            result = result[0]

    return jsonify({
        "status": "success",
        "data": result
    })



@dt_model.route('/dt_safety_modeling/<camera_id>', methods=['GET'])
def dt_safety_modeling_data_api(camera_id):
    """DT 모델링 데이터를 반환하는 API (특정 카메라 ID 기반)"""

    if not camera_id:
        return jsonify({"status": "error", "message": "Missing required parameter: camera_id"}), 400
    
    response = select_safety_detected_object_dimensions(camera_id)

    return response




@dt_model.route('/dt_modeling/<camera_id>', methods=['GET'])
def dt_modeling_data_api(camera_id):
    """DT 모델링 데이터를 반환하는 API (특정 카메라 ID 기반)"""

    if not camera_id:
        return jsonify({"status": "error", "message": "Missing required parameter: camera_id"}), 400
    
    base_time = request.args.get('base_time')

    print(base_time)

    # 특정 카메라 ID에 대한 탐지된 객체 정보 조회
    if base_time == None or base_time == "" or base_time == '""':
        response = select_detected_object_dimensions(camera_id)
    else:
        response = select_detected_object_dimensions(camera_id, base_time)

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)


@dt_model.route('/inventories/', methods=['GET'])
def dt_inventories_data_api():
    """DT 모델링 전체 재고list를 반환하는 API """

    base_time = request.args.get('base_time')
    sim_id = request.args.get('sim_id')

    print(base_time)

    # 특정 카메라 ID에 대한 탐지된 객체 정보 조회
    if base_time == None or base_time == "" or base_time == '""':
        response = select_detected_object_inventories(sim_id)
    else:
        response = select_detected_object_inventories(sim_id, base_time)

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)


@dt_model.route('/inventories_hoseon/', methods=['GET'])
def dt_inventories_hoseon_data_api():
    """DT 모델링 전체 재고list(호선)를 반환하는 API """

    base_time = request.args.get('base_time')
    sim_id = request.args.get('sim_id')

    # print(base_time)

    # 특정 카메라 ID에 대한 탐지된 객체 정보 조회
    if base_time == None or base_time == "" or base_time == '""':
        response = select_detected_object_inventories_hoseon(sim_id)
    else:
        response = select_detected_object_inventories_hoseon(sim_id, base_time)
    # print(response)
    response1 = get_filter_label_counts()
    # print(response1)

    response["data"] = subtract_label_counts(response["data"], response1["data"])

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)



@dt_model.route('/dt_modeling/by_image/<image_id>', methods=['GET'])
def dt_return_img_api(image_id):
    """이미지를 base64 형식으로 반환하는 API (이미지 ID 기반)"""

    detection_id = request.args.get('detection_id')

    if not image_id:
        return jsonify({"status": "error", "message": "Missing required parameter: image_id"}), 400

    if not detection_id:
        return jsonify({"status": "error", "message": "Missing required parameter: detection_id"}), 400

    # 특정 카메라 ID에 대한 탐지된 객체 정보 조회
    response = return_selected_object_imgs(image_id, detection_id)

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)

@dt_model.route('/dt_grid_info/collision_events', methods=['GET'])
def dt_collision_events():
    """충돌 관련 이벤트 데이터를 반환하는 API"""

    # 요청 파라미터 가져오기
    camera_id = request.args.get('camera_id')
    limit = request.args.get('limit', default=10, type=int)
    status = request.args.get('status')

    # 충돌 이벤트 데이터 조회
    response = select_collision_events(camera_id=camera_id, limit=limit, status=status)

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)





#save_user_input_data 사용자 입력 객체 정보 추가
@dt_model.route('/save_user_input_data', methods=['POST'])
def save_user_input_data():
    """
    사용자 입력 그리드 정보를 추가함.
    """
    data = request.get_json()

    required_fields = [
        "camera_id", "object_label", "grid_width", "grid_height",
        "input_row", "input_col", "order_no", "created_at", "created_by", "updated_by"
    ]

    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({'error': f'Missing fields: {", ".join(missing)}'}), 400


    if 'image_id' not in data.keys():
        # 신규 image_id 생성
        print('test')
        data['image_id'] = get_new_image_id(data['camera_id'])
        next_input_id = get_next_input_id(data['image_id'])
        insert_user_input_data(data, next_input_id)
        return jsonify({'message': 'Insert successful',
                        'image_id': data['image_id'],
                        'input_id': next_input_id}), 201
    else:
        try:
            next_input_id = get_next_input_id(data['image_id'])
            insert_user_input_data(data, next_input_id)
            return jsonify({'message': 'Insert successful', 'input_id': next_input_id}), 201
        except Exception as e:
            return jsonify({'error': str(e)}), 500





#사용자 입력 정보 가져오기기
@dt_model.route('/get_user_input_data', methods=['GET'])
def get_user_input_data():
    """
    image_id에 사용자 입력 정보를 반환환
    """
    image_id = request.args.get('image_id')  # 쿼리파라미터 예: /select?image_id=IMG001

    if not image_id:
        return jsonify({"status": "error", "message": "Missing required parameter: image_id"}), 400

    response = select_user_input_data(image_id)

    if response["status"] != "success":
        return jsonify(response), 500

    return jsonify(response)


#사용자 입력 정보 업데이트
@dt_model.route('/update_user_input_data', methods=['POST'])
def update_user_input_data():
    """
    사용자 입력 정보를 업데이트함함.
    """
    data = request.get_json()
    input_id = data.get("input_id")
    image_id = data.get("image_id")

    if not input_id:
        return jsonify({"error": "input_id is required for update"}), 400
    if not image_id:
        return jsonify({"error": "image_id is required for update"}), 400

    response = updateUserInputData(image_id, input_id, data)

    if response != 1:
        return jsonify({"message": "Update fail", "image_id": image_id, "input_id": input_id}), 500

    return jsonify({"message": "Update successful", "image_id": image_id, "input_id": input_id})




#사용자 입력 정보 삭제제
@dt_model.route('/delete_user_input_data', methods=['DELETE'])
def delete_user_input_data():
    """
    사용자 입력 정보를 업데이트함함.
    """
    image_id = request.args.get('image_id')
    input_id = request.args.get('input_id')

    if not input_id:
        return jsonify({"error": "input_id is required for update"}), 400
    if not image_id:
        return jsonify({"error": "image_id is required for update"}), 400

    response = DeleteUserInputData(image_id, input_id)

    if response != 1:
        return jsonify({"message": "Delete fail", "image_id": image_id, "input_id": input_id}), 500

    return jsonify({"message": "Delete successful", "image_id": image_id, "input_id": input_id})
