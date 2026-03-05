from flask import Blueprint, request, jsonify
from grid.lib.grid_func import *
from blueprints.lib.public_func import find_largest_jpg_file, get_image_size
import os
import cv2
import base64
import uuid
import logging
from PIL import Image
import io
from datetime import datetime, timedelta


# 0. BluePrint
# Blueprint 정의
grid_crud = Blueprint('grid_crud', __name__, url_prefix='/grid/grid_crud')

# 1. 전역 상태 관리

#loc_states: 이미지별 격자 상태 저장
loc_states = {}  # Grid state storage


# 2. 초기화 및 기본 설정

#initialize_coordinates : 초기 격자 좌표를 설정. use_click 여부에 따라 클릭 지점 기반 탐지 또는 이미지 전체 탐지.
@grid_crud.route('/initialize_coordinates', methods=['POST'])
def initialize_coordinates():
    """
    Initialize grid coordinates based on the provided settings.
    """
    global loc_states

    # try:
    data = request.json
    if not data:
        return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

    image_base64 = data.get('image_base64')
    click_coordinates = data.get('click_coordinates')
    search_radius = data.get('search_radius')
    print(click_coordinates)
    if not image_base64:
        return jsonify({"success": False, "code": 404, "msg": "Image Base64 data is required."}), 400

    # Decode the original image from Base64 and save it
    image = decode_base64_to_image(image_base64)
    if image is None:
        return jsonify({"success": False, "code": 404, "msg": "Failed to decode image."}), 404

    origin_image_base64 = image_base64  # Save the original image Base64 string

    # Save the decoded image to a file
    file_name = os.path.join(os.getcwd(), "decoded_image.jpg")
    cv2.imwrite(file_name, image)

    # Detect initial grid
    if click_coordinates[0] == None:
        approx = detect_red_squares(file_name)
    else:
        approx, message = detect_red_squares_near_point(file_name, click_coordinates, click_mouse=True)
        print(message)
    if approx is None:
        return jsonify({"success": False, "code": 404, "msg": "No red squares detected in the image."}), 404

    initial_coordinates = [{"row": 0, "col": idx, "coordinates": square.tolist()} for idx, square in enumerate([approx])]

    # Visualize the detected grid on the image
    output_image = show_approx([[np.array(approx, dtype=np.int32)]], image, display_labels=False)
    _, buffer = cv2.imencode('.jpg', output_image)
    updated_image_base64 = base64.b64encode(buffer).decode('utf-8')

    unique_id = str(uuid.uuid4())
    loc_states[unique_id] = {
        "initial_coordinates": initial_coordinates,
        "sort_direction": "up",
        "approx_list": [[np.array(square, dtype=np.int32)] for square in [approx]],
        "image_base64": updated_image_base64,  # Processed image
        "origin_image_base64": origin_image_base64,  # Original image
        "extend_count": {"up": 0, "down": 0, "left": 0, "right": 0},
        "last_accessed": datetime.now(),
        "updated": False
    }
    print('approx_list', loc_states[unique_id]["approx_list"])

    return jsonify({
        "success": True,
        "msg": "성공하였습니다.",
        "initial_coordinates": initial_coordinates,
        "image_base64": updated_image_base64,
        "unique_id": unique_id,
        "code": 200
    }), 200

    # except Exception as e:
    #     return jsonify({"success": False, "message": str(e)}), 500





# 3. 정렬 방향

#update_sort_direction : 특정 이미지 또는 전역 정렬 방향 업데이트
@grid_crud.route('/update_sort_direction', methods=['POST'])
def update_sort_direction():
    """
    Update the sort direction for the specified unique_id.
    """
    try:
        data = request.json

        # Check if the request data exists
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        unique_id = data.get('unique_id')
        new_sort_direction = data.get('sort_direction')

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Validate sort_direction
        if new_sort_direction not in ['up', 'down', 'left', 'right']:
            return jsonify({"success": False, "code": 400, "msg": "Invalid sort_direction value."}), 400

        # Update sort_direction in loc_states
        loc_states[unique_id]["sort_direction"] = new_sort_direction

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다."
        }), 200

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except Exception as e:
        # Log other unhandled exceptions
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500


# 4. 격자 작업

#process_grid : 격자 확장/축소 작업 처리
@grid_crud.route('/process_grid', methods=['POST'])
def process_grid():
    """
    Process grid operations (extend/shrink) based on the provided commands.
    Ensures the grid does not shrink beyond the extend limit.
    """
    global loc_states

    try:
        data = request.json

        # Check if request data exists
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        unique_id = data.get('unique_id')
        operations = data.get('operations', [])

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Retrieve the grid state
        grid_state = loc_states.get(unique_id)
        if not grid_state:
            return jsonify({"success": False, "code": 404, "msg": "Grid state not found for the given unique_id."}), 404

        approx_list = grid_state.get("approx_list", [])
        sort_direction = grid_state.get("sort_direction", "up")
        extend_count = grid_state.get("extend_count", {"up": 0, "down": 0, "left": 0, "right": 0})
        # vanishing_point_enabled = grid_state.get("vanishing_point_enabled", True)  # Default to False if not set

        # Process operations to update the grid
        for action, count in operations:
            if action == "up_extend":
                # approx_list = up_extend(sort_direction, approx_list, count, vanishing_point_enabled)
                approx_list = up_extend(sort_direction, approx_list, count)
                extend_count["up"] += count
            elif action == "down_extend":
                # approx_list = down_extend(sort_direction, approx_list, count, vanishing_point_enabled)
                approx_list = down_extend(sort_direction, approx_list, count)
                extend_count["down"] += count
            elif action == "left_extend":
                # approx_list = left_extend(sort_direction, approx_list, count, vanishing_point_enabled)
                approx_list = left_extend(sort_direction, approx_list, count)
                extend_count["left"] += count
            elif action == "right_extend":
                # approx_list = right_extend(sort_direction, approx_list, count, vanishing_point_enabled)
                approx_list = right_extend(sort_direction, approx_list, count)
                extend_count["right"] += count
            elif action == "up_shrink":
                if extend_count["up"] > 0:
                    approx_list = up_shrink(approx_list, sort_direction)
                    extend_count["up"] -= 1
                else:
                    raise ValueError("Cannot shrink further in the 'up' direction.")
            elif action == "down_shrink":
                if extend_count["down"] > 0:
                    approx_list = down_shrink(approx_list, sort_direction)
                    extend_count["down"] -= 1
                else:
                    raise ValueError("Cannot shrink further in the 'down' direction.")
            elif action == "left_shrink":
                if extend_count["left"] > 0:
                    approx_list = left_shrink(approx_list, sort_direction)
                    extend_count["left"] -= 1
                else:
                    raise ValueError("Cannot shrink further in the 'left' direction.")
            elif action == "right_shrink":
                if extend_count["right"] > 0:
                    approx_list = right_shrink(approx_list, sort_direction)
                    extend_count["right"] -= 1
                else:
                    raise ValueError("Cannot shrink further in the 'right' direction.")
            else:
                raise ValueError(f"Invalid action: {action}")

        # Update the grid state
        grid_state["approx_list"] = approx_list
        grid_state["extend_count"] = extend_count

        # Always use the original image for re-rendering the grid
        original_image = decode_base64_to_image(grid_state["origin_image_base64"])
        updated_image = show_approx(approx_list, original_image, sort_direction)

        # Encode the updated image back to Base64
        _, buffer = cv2.imencode('.jpg', updated_image)
        updated_image_base64 = base64.b64encode(buffer).decode('utf-8')

        # Update img_data in loc_states
        grid_state["image_base64"] = updated_image_base64
        loc_states[unique_id] = grid_state
        loc_states[unique_id]["updated"] = True
        loc_states[unique_id]["last_accessed"] = datetime.now()

        # Return the response with updated and initial coordinates
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "updated_coordinates": generate_coordinates(approx_list, sort_direction),
            "image_base64": updated_image_base64
        }), 200

    except KeyError as e:
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        return jsonify({"success": False, "code": 400, "msg": f"Invalid input: {str(e)}"}), 400

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500





# 5. 저장 및 로드

#save_output_image_api : 현재 격자 상태와 이미지를 저장.
@grid_crud.route('/save_grid', methods=['POST'])
def save_output_image_api():
    """
    Save the current grid state to PostgreSQL, including metadata and images (Base64 encoded).
    If org_img_data is updated in the current state, it will also be updated in the database.
    """
    try:
        # PostgreSQL 연결
        conn = get_connection()
        if conn is None:
            return jsonify({"success": False, "code": 500, "msg": "데이터베이스 연결에 실패했습니다."}), 500

        cursor = conn.cursor()

        # 요청 데이터 확인
        data = request.json
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        # 프론트엔드에서 제공하는 필드
        camera_id = data.get('camera_id')
        grid_unit = data.get("grid_unit")
        created_by = data.get('created_by')
        updated_by = data.get('updated_by')
        bwts_pro_run = data.get('bwts_pro_run')
        scr_pro_run = data.get('scr_pro_run')
        bwts_mat_run = data.get('bwts_mat_run')
        scr_mat_run = data.get('scr_mat_run')
        nox_run = data.get('nox_run')
        pre_absorber_run = data.get('pre_absorber_run')

        if not all([camera_id, created_by, updated_by]):
            return jsonify({"success": False, "code": 400, "msg": "Missing required fields (camera_id, created_by, updated_by)"}), 400

        unique_id = data.get('unique_id')

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Retrieve the grid state from loc_states
        current_state = loc_states.get(unique_id)
        if not current_state:
            return jsonify({"success": False, "code": 404, "msg": "Grid state not found for the given unique_id."}), 404

        # Extract necessary data from the current state
        updated_image_base64 = current_state.get("image_base64")  # 최신 이미지 (img_data)
        origin_image_base64 = current_state.get("origin_image_base64")  # 초기 이미지 (org_img_data)
        initial_coordinates = current_state.get("initial_coordinates")
        approx_list = current_state.get("approx_list")  # approx_list 데이터 가져오기
        sort_direction = current_state.get("sort_direction", "up")
        extend_count = current_state.get("extend_count", {"up": 0, "down": 0, "left": 0, "right": 0})  # extend_count 가져오기

        if not updated_image_base64:
            return jsonify({"success": False, "code": 404, "msg": "image_base64 is missing in the grid state."}), 404

        # Generate grid coordinates
        grid_coordinates = generate_coordinates(approx_list, sort_direction)

        # 현재 시간으로 updated_at 설정
        current_time = datetime.now()

        # PostgreSQL에 데이터 저장 또는 업데이트
        insert_query = """
        INSERT INTO tb_camera_grid (camera_id, grid_data, img_data, org_img_data, sort_direction, grid_unit, extend_count, created_at, created_by, updated_at, updated_by, bwts_pro_run, scr_pro_run, bwts_mat_run, scr_mat_run, nox_run, pre_absorber_run)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (camera_id) 
        DO UPDATE SET 
            grid_data = EXCLUDED.grid_data,
            img_data = EXCLUDED.img_data,                -- 최신 이미지로 업데이트
            org_img_data = EXCLUDED.org_img_data,        -- 초기 이미지 업데이트
            sort_direction = EXCLUDED.sort_direction,
            grid_unit = EXCLUDED.grid_unit,
            extend_count = EXCLUDED.extend_count,        -- extend_count 업데이트
            updated_at = EXCLUDED.updated_at,
            updated_by = EXCLUDED.updated_by,
            bwts_pro_run = EXCLUDED.bwts_pro_run,
            scr_pro_run = EXCLUDED.scr_pro_run,
            bwts_mat_run = EXCLUDED.bwts_mat_run,
            scr_mat_run = EXCLUDED.scr_mat_run,
            nox_run = EXCLUDED.nox_run,
            pre_absorber_run = EXCLUDED.pre_absorber_run
        """
        cursor.execute(insert_query, (
            camera_id,
            json.dumps(approx_list, default=lambda x: x.tolist()),  # approx_list를 JSON 문자열로 저장
            updated_image_base64,  # 최신 이미지 저장 (img_data)
            origin_image_base64,   # 초기 이미지 저장 (org_img_data)
            sort_direction,
            grid_unit,
            json.dumps(extend_count),  # extend_count를 JSON 문자열로 저장
            current_time,  # created_at에 현재 시간 저장
            created_by,
            current_time,  # updated_at에 현재 시간 저장
            updated_by,
            bwts_pro_run,
            scr_pro_run,
            bwts_mat_run,
            scr_mat_run,
            nox_run,
            pre_absorber_run 
        ))
        conn.commit()

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "grid_data": grid_coordinates
        }), 200

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid operation or value errors
        return jsonify({"success": False, "code": 400, "msg": f"Invalid input: {str(e)}"}), 400

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Closed to PostgreSQL successfully.")

# OpenCV 컨투어 스타일 to 일반 2D 좌표 리스트
def convert_nested_coordinates(data):
    # 5차 중첩 → 4차 중첩
    return [
        [  # 1차 그룹
            [  # 2차 셀
                point[0]  # [[[x, y]]] → [x, y]
                for point in polygon
            ]
            for polygon in row
        ]
        for row in data
    ]

#일반 2D 좌표 리스트 to OpenCV 컨투어 스타일
def convert_to_opencv_format(data):
    # 4차 중첩 → 5차 중첩 (좌표를 [[x, y]] 형식으로 감쌈)
    return [
        [  # 1차 그룹
            [  # 2차 셀
                [point]  # [x, y] → [[x, y]]
                for point in polygon
            ]
            for polygon in row
        ]
        for row in data
    ]

#get_raw_img : 스테이트 변수에서 원본이미지 로드 초기화용도도.
@grid_crud.route('/get_raw_img', methods=['POST'])
def get_raw_img():
    global loc_states
     
    try:
        data = request.json
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404
        
        data = request.json
        unique_id = data.get('unique_id')

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Retrieve the grid state from loc_states
        current_state = loc_states.get(unique_id)
        if not current_state:
            return jsonify({"success": False, "code": 404, "msg": "Grid state not found for the given unique_id."}), 404

        #이미지 사이즈
        image_data = base64.b64decode(current_state['origin_image_base64'])
        image_array = np.frombuffer(image_data, dtype=np.uint8)
        image = Image.open(io.BytesIO(image_array))
        width, height = image.size
        # height와 width의 최대공약수(GCD) 구하기
        gcd = math.gcd(width, height)
        # 비율 계산
        width_ratio = width // gcd
        height_ratio = height // gcd

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            'img_size': {
                'width': width,
                'height': height,
            },
            'ratio':  f"{width_ratio}:{height_ratio}",
            "origin_image_base64": current_state["origin_image_base64"],
        }), 200
    
    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid input data
        return jsonify({"success": False, "code": 404, "msg": f"Invalid input: {str(e)}"}), 404

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500



#load_grid_state : JSON 파일에서 저장된 격자 상태를 로드.
@grid_crud.route('/load_grid', methods=['POST'])
def load_grid_state():
    """
    Load a previously saved grid state from PostgreSQL using camera_id.
    A new unique_id is generated for the loaded state.
    """
    try:
        # PostgreSQL 연결
        conn = get_connection()
        if conn is None:
            return jsonify({"success": False, "code": 500, "msg": "데이터베이스 연결에 실패했습니다."}), 500

        cursor = conn.cursor()

        # 요청 데이터 확인
        data = request.json
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        camera_id = data.get('camera_id')

        # Validate camera_id
        if not camera_id:
            return jsonify({"success": False, "code": 404, "msg": "camera_id is required."}), 404

        # DB에서 grid 상태 조회
        select_query = """
        SELECT grid_data, 
                img_data, 
                sort_direction, 
                grid_unit, 
                org_img_data, 
                updated_at, 
                extend_count,
                bwts_pro_run,
                scr_pro_run,
                bwts_mat_run,
                scr_mat_run,
                nox_run,
                pre_absorber_run
        FROM tb_camera_grid
        WHERE camera_id = %s
        """
        cursor.execute(select_query, (camera_id,))
        result = cursor.fetchone()

        if result is None:
            return jsonify({"success": False, "code": 404, "msg": "No grid state found for the given camera_id."}), 404

        # 결과 처리
        grid_data = result[0]   # grid_data 
        image_base64 = result[1]           # img_data (Base64 인코딩된 이미지)
        sort_direction = result[2]         # sort_direction
        grid_unit = result[3]              # grid_unit
        origin_image_base64 = result[4]    # org_img_data (원본 이미지)
        updated_at = result[5]             # updated_at
        extend_count = result[6]           # 이미 딕셔너리로 반환된 extend_count
        bwts_pro_run = result[7]
        scr_pro_run = result[8]
        bwts_mat_run = result[9]
        scr_mat_run = result[10]
        nox_run = result[11]
        pre_absorber_run = result[12]

        # Process grid_data into approx_list
        approx_list = []
        for row_idx, row in enumerate(grid_data):
            approx_row = []
            for col_idx, col in enumerate(row):
                if col is not None:
                    approx_row.append(np.array(col, dtype=np.int32))
                else:
                    approx_row.append(None)
            approx_list.append(approx_row)

        # 새로운 unique_id 생성
        unique_id = str(uuid.uuid4())

        #이미지 사이즈 전송
        image_data = base64.b64decode(image_base64)
        image_array = np.frombuffer(image_data, dtype=np.uint8)
        image = Image.open(io.BytesIO(image_array))
        width, height = image.size
        # height와 width의 최대공약수(GCD) 구하기
        gcd = math.gcd(width, height)
        # 비율 계산
        width_ratio = width // gcd
        height_ratio = height // gcd

        # loc_states에 상태 저장 (unique_id를 키로 사용)
        loc_states[unique_id] = {
            "camera_id": camera_id,
            "initial_coordinates": grid_data,
            "sort_direction": sort_direction,
            "approx_list": approx_list,
            "image_base64": image_base64,
            "origin_image_base64": origin_image_base64,
            "grid_unit": grid_unit,
            "extend_count": extend_count,  # extend_count 복원
            "bwts_pro_run": bwts_pro_run,
            "scr_pro_run" : scr_pro_run,
            "bwts_mat_run" : bwts_mat_run,
            "scr_mat_run" : scr_mat_run,
            "nox_run" : nox_run,
            "pre_absorber_run" : pre_absorber_run,
            "last_accessed": datetime.now(),
            "updated": False
        }

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "unique_id": unique_id,
            "camera_id": camera_id,
            "initial_coordinates": loc_states[unique_id]["initial_coordinates"],
            "sort_direction": loc_states[unique_id]["sort_direction"],
            "image_base64": loc_states[unique_id]["image_base64"],
            'img_size': {
                'width': width,
                'height': height,
            },
            'ratio':  f"{width_ratio}:{height_ratio}",
            "origin_image_base64": loc_states[unique_id]["origin_image_base64"],
            "grid_unit": loc_states[unique_id]["grid_unit"],
            "extend_count": loc_states[unique_id]["extend_count"],
            "bwts_pro_run": bwts_pro_run,
            "scr_pro_run" : scr_pro_run,
            "bwts_mat_run" : bwts_mat_run,
            "scr_mat_run" : scr_mat_run,
            "nox_run" : nox_run,
            "pre_absorber_run" : pre_absorber_run,
            "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S")
        }), 200

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid input data
        return jsonify({"success": False, "code": 404, "msg": f"Invalid input: {str(e)}"}), 404

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


#save_grid_unit : 특정 이미지에 대한 grid_unit 값을 저장/업데이트
@grid_crud.route('/save_grid_unit', methods=['POST'])
def save_grid_unit():
    """
    Save or update the grid_unit value for a specific unique_id.
    """
    try:
        data = request.json

        # Check if request data exists
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        unique_id = data.get('unique_id')
        grid_unit = data.get('grid_unit')

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Validate grid_unit
        if grid_unit is None:
            return jsonify({"success": False, "code": 404, "msg": "grid_unit value is required."}), 404

        # Update grid_unit in loc_states
        loc_states[unique_id]["grid_unit"] = grid_unit

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "grid_unit": grid_unit
        }), 200

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid grid_unit values
        return jsonify({"success": False, "code": 404, "msg": f"Invalid grid_unit value: {str(e)}"}), 404

    except Exception as e:
        # Log unhandled exceptions
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

# 6. 유틸리티

#get_cctv_img : 테스트 이미지 반환, 이미지 크기, GCD 기반 비율, Base64 데이터 제공
@grid_crud.route('/get_test_img', methods=['GET'])
def get_cctv_img():
    """
    Get a test image, return its base64 encoding, dimensions, and aspect ratio.
    """
    try:
        # 이미지 경로 설정
        out_path = os.getcwd() + '/grid/1B.png'

        print(out_path)

        # Validate if the file exists
        if not os.path.exists(out_path):
            return jsonify({"success": False, "code": 404, "msg": "Image file not found."}), 404


        # 이미지 크기 확인
        width, height = get_image_size(out_path)
        if width <= 0 or height <= 0:
            return jsonify({"success": False, "code": 404, "msg": "Invalid image dimensions."}), 404

        # 이미지 파일 로드
        with open(out_path, 'rb') as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

        # 최대공약수(GCD) 계산
        gcd = math.gcd(width, height)

        # 비율 계산
        width_ratio = width // gcd
        height_ratio = height // gcd

        # 응답으로 JSON 반환
        response = {
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            'image_base64': encoded_image,
            'img_size': {
                'width': width,
                'height': height,
            },
            'ratio': f"{width_ratio}:{height_ratio}"
        }
        return jsonify(response)

    except FileNotFoundError:
        # 파일이 없을 경우 처리
        return jsonify({"success": False, "code": 404, "msg": "Image file not found."}), 404

    except ValueError as e:
        # 잘못된 값이 발생했을 경우 처리
        return jsonify({"success": False, "code": 404, "msg": f"Invalid value: {str(e)}"}), 404

    except Exception as e:
        # 기타 예외 처리
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 404, "msg": "An unexpected error occurred.", "details": str(e)}), 500


@grid_crud.route('/get_grid_state_summary', methods=['GET'])
def get_grid_state_summary():
    """
    Retrieve the summary of cached grid states including UUID, updated status, and last accessed time.
    """
    try:
        if not loc_states:
            return jsonify({"success": False, "code": 404, "msg": "No cached grid states found."}), 404

        summary = [
            {
                "unique_id": unique_id,
                "updated": grid_state.get("updated", False),
                "last_accessed": grid_state.get("last_accessed").strftime("%Y-%m-%d %H:%M:%S")
            }
            for unique_id, grid_state in loc_states.items()
        ]

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "Grid state summary retrieved successfully.",
            "grid_state_summary": summary
        }), 200

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

def clean_up_non_updated_states():
    """
    Periodically clean up grid states where 'updated' is False
    and 'last_accessed' is older than 1 minute.
    """
    global loc_states

    now = datetime.now()
    threshold_time = now - timedelta(days=7)  # 7일 전 시간 계산

    non_updated_keys = []

    for key, state in loc_states.items():
        last_accessed = state.get("last_accessed")

        # last_accessed가 문자열이면 datetime으로 변환
        if isinstance(last_accessed, str):
            last_accessed = datetime.strptime(last_accessed, "%Y-%m-%d %H:%M:%S")

        # 'last_accessed'가 7분 이상 지난 경우 삭제 대상에 추가
        if last_accessed < threshold_time:
            non_updated_keys.append(key)

    # 식별된 항목 삭제
    for key in non_updated_keys:
        del loc_states[key]

    print(f"Deleted {len(non_updated_keys)} non-updated grid states older than 7 days.")

# 소실점 on/off 설정
@grid_crud.route('/set_vanishing_point', methods=['POST'])
def set_vanishing_point():
    """
    Enable or disable vanishing point consideration for a specific grid state.
    """
    try:
        data = request.json

        # Validate input data
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        unique_id = data.get('unique_id')
        enable_vanishing_point = data.get('enable_vanishing_point')

        # Validate unique_id
        if not unique_id or unique_id not in loc_states:
            return jsonify({"success": False, "code": 404, "msg": "Valid unique_id is required."}), 404

        # Validate enable_vanishing_point
        if not isinstance(enable_vanishing_point, bool):
            return jsonify({"success": False, "code": 400, "msg": "Invalid enable_vanishing_point value. Must be boolean."}), 400

        # Update vanishing point state in loc_states
        loc_states[unique_id]["vanishing_point_enabled"] = enable_vanishing_point

        return jsonify({
            "success": True,
            "code": 200,
            "msg": f"Vanishing point consideration {'enabled' if enable_vanishing_point else 'disabled'} successfully.",
        }), 200

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500



# 가장 안쪽에 있는 [x, y] 점의 개수를 세는 함수
def count_points(structure):
    if isinstance(structure, list) and all(isinstance(item, list) and len(item) == 2 for item in structure):
        return len(structure)
    elif isinstance(structure, list):
        for item in structure:
            return count_points(item)
    return 0

# 안전관련 격자 생성
@grid_crud.route('/point_list_view', methods=['POST'])
def point_list_view():
    """
    Enable or disable vanishing point consideration for a specific grid state.
    """
    global loc_states

    try:
        data = request.json

        # Validate input data
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        point_list = data.get('point_list')
        image_base64 = data.get('image_base64')
        sort_direction = data.get('sort_direction')

        # Decode the original image from Base64 and save it
        image = decode_base64_to_image(image_base64)

        # Validate enable_vanishing_point
        if not isinstance(point_list, list):
            return jsonify({"success": False, "code": 400, "msg": "Invalid enable_vanishing_point value. Must be boolean."}), 400

        # Update vanishing point state in loc_states
        print(point_list)

        ## Initialize empty grid and point buffer
        grid = []
        point_buffer = []

        # Generate grid by adding points sequentially
        for point in point_list:
            grid = generate_grid(grid, point_buffer, point)
            # print('check_grid_row_consistency:::', check_grid_row_consistency(grid))

         # Finalize by adding last remaining buffer
        finalize_grid(grid, point_buffer)

        # #정렬 변환
        sort_grid_ = sort_grid(grid, order=sort_direction)

        # Draw grid on image
        result_image = draw_grid_on_image(image, sort_grid_)
        _, buffer = cv2.imencode('.jpg', result_image)
        view_image_base64 = base64.b64encode(buffer).decode('utf-8')
        

        if len(point_list) == 4:
            flattened = sort_grid_[0][0]
            transformed = [[[x, y]] for x, y in flattened]
            initial_coordinates = [{"row": 0, "col": 0, "coordinates": transformed}]    
            unique_id = str(uuid.uuid4())
            loc_states[unique_id] = {
                "initial_coordinates": initial_coordinates,
                "sort_direction": "up",
                "approx_list": [[np.array(transformed, dtype=np.int32)]],
                "image_base64": view_image_base64,  # Processed image
                "origin_image_base64": image_base64,  # Original image
                "extend_count": {"up": 0, "down": 0, "left": 0, "right": 0},
                "last_accessed": datetime.now(),
                "updated": False
            }
            print('approx_list', loc_states[unique_id]["approx_list"])
        else:
            initial_coordinates = []
            unique_id = None

           
        print('initial_coordinates', initial_coordinates)

        return jsonify({
            "success": True,
            "code": 200,
            "view_image_base64": view_image_base64,
            "initial_coordinates": initial_coordinates,
            "unique_id": unique_id,
            "row_consistency": check_grid_row_consistency(grid),
            "msg": f" Processing complete",
        }), 200

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500



#save_output_image_api : 현재 격자 상태와 이미지를 저장.
@grid_crud.route('/save_safety_grid', methods=['POST'])
def save_safety_grid():
    """
    안전 관련 그리드 저장
    """
    try:
        # PostgreSQL 연결
        conn = get_connection()
        if conn is None:
            return jsonify({"success": False, "code": 500, "msg": "데이터베이스 연결에 실패했습니다."}), 500

        cursor = conn.cursor()

        # 요청 데이터 확인
        data = request.json
        if not data:
            return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

        # 프론트엔드에서 제공하는 필드
        camera_id = data.get('camera_id')
        sort_direction = data.get("sort_direction")
        point_list_data = data.get("point_list_data")
        setMode = data.get('setMode') # 0: 수동, 1: 수동 + 자동모드
        unique_id = data.get('unique_id')
        grid_unit = data.get("grid_unit")
        created_by = data.get('created_by')
        updated_by = data.get('updated_by')

        print('setMode', setMode)
        print('unique_id', unique_id)
        
        # 수동모드드
        if setMode == '0':

            if not all([camera_id, sort_direction, point_list_data, grid_unit, created_by, updated_by]):
                return jsonify({"success": False, "code": 400, "msg": "Missing required fields (camera_id, sort_direction, point_list_data, grid_unit, created_by, updated_by)"}), 400

            #그리드 생성
            ## Initialize empty grid and point buffer
            grid = []
            point_buffer = []

            # Generate grid by adding points sequentially
            for point in point_list_data:
                grid = generate_grid(grid, point_buffer, point)
                # print('check_grid_row_consistency:::', check_grid_row_consistency(grid))

            # Finalize by adding last remaining buffer
            finalize_grid(grid, point_buffer)

            # #정렬 변환
            sort_grid_ =  convert_to_opencv_format(sort_grid(grid, order=sort_direction))


            # 현재 시간으로 updated_at 설정
            current_time = datetime.now()

            # PostgreSQL에 데이터 저장 또는 업데이트
            insert_query = """
                INSERT INTO tb_camera_safety_grid (camera_id, grid_data, sort_direction, grid_unit, point_list_data, set_mode, created_at, created_by, updated_at, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (camera_id) 
                DO UPDATE SET 
                    grid_data = EXCLUDED.grid_data,
                    sort_direction = EXCLUDED.sort_direction,                -- 그리드 정렬 방향
                    grid_unit = EXCLUDED.grid_unit,        -- 그리드 격자 단위
                    point_list_data = EXCLUDED.point_list_data,        -- 포인트 LIST
                    set_mode = EXCLUDED.set_mode,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
            """
            cursor.execute(insert_query, (
                camera_id,
                json.dumps(sort_grid_),  # approx_list를 JSON 문자열로 저장
                sort_direction,  # 방향
                grid_unit,   # 길이 단위
                json.dumps(point_list_data),  # extend_count를 JSON 문자열로 저장
                setMode,
                current_time,  # created_at에 현재 시간 저장
                created_by,
                current_time,  # updated_at에 현재 시간 저장
                updated_by
            ))
            conn.commit()

            return jsonify({
                "success": True,
                "code": 200,
                "msg": "성공하였습니다.",
                "input_data": {
                    "camera_id" : camera_id,
                    "sort_direction" : sort_direction,
                    "point_list_data" : point_list_data,
                    "grid_unit" : grid_unit,
                    }
            }), 200
        #수동 + 자동모드
        else:
            if not all([camera_id, sort_direction, point_list_data, grid_unit, unique_id, created_by, updated_by]):
                return jsonify({"success": False, "code": 400, "msg": "Missing required fields (camera_id, sort_direction, point_list_data, grid_unit, created_by, updated_by)"}), 400

            # #정렬 변환

            sort_grid_ = json.dumps(loc_states[unique_id]["approx_list"], default=lambda x: x.tolist())
            extend_count = loc_states[unique_id]["extend_count"]

            # 현재 시간으로 updated_at 설정
            current_time = datetime.now()

            # PostgreSQL에 데이터 저장 또는 업데이트
            insert_query = """
                INSERT INTO tb_camera_safety_grid (camera_id, grid_data, sort_direction, grid_unit, point_list_data, set_mode, extend_count, created_at, created_by, updated_at, updated_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (camera_id) 
                DO UPDATE SET 
                    grid_data = EXCLUDED.grid_data,
                    sort_direction = EXCLUDED.sort_direction,                -- 그리드 정렬 방향
                    grid_unit = EXCLUDED.grid_unit,        -- 그리드 격자 단위
                    point_list_data = EXCLUDED.point_list_data,        -- 포인트 LIST
                    set_mode = EXCLUDED.set_mode,
                    extend_count = EXCLUDED.extend_count,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
            """
            cursor.execute(insert_query, (
                camera_id,
                sort_grid_,  # approx_list를 JSON 문자열로 저장
                sort_direction,  # 방향
                grid_unit,   # 길이 단위
                json.dumps(point_list_data),  # extend_count를 JSON 문자열로 저장
                setMode,
                json.dumps(extend_count),
                current_time,  # created_at에 현재 시간 저장
                created_by,
                current_time,  # updated_at에 현재 시간 저장
                updated_by
            ))
            conn.commit()

            return jsonify({
                "success": True,
                "code": 200,
                "msg": "성공하였습니다.",
                "input_data": {
                    "camera_id" : camera_id,
                    "sort_direction" : sort_direction,
                    "point_list_data" : point_list_data,
                    "grid_unit" : grid_unit,
                    }
            }), 200
        

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid operation or value errors
        return jsonify({"success": False, "code": 400, "msg": f"Invalid input: {str(e)}"}), 400

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")


#디비에 저장된 격자 로드드
@grid_crud.route('/load_safety_grid', methods=['POST'])
def load_safety_grid():
    """
    Load a previously saved grid state from PostgreSQL using camera_id.
    A new unique_id is generated for the loaded state.
    """

    # PostgreSQL 연결
    conn = get_connection()
    if conn is None:
        return jsonify({"success": False, "code": 500, "msg": "데이터베이스 연결에 실패했습니다."}), 500

    cursor = conn.cursor()

    # 요청 데이터 확인
    data = request.json
    if not data:
        return jsonify({"success": False, "code": 404, "msg": "No data provided."}), 404

    camera_id = data.get('camera_id')
    image_base64 = data.get('image_base64')

    # Validate camera_id
    if not camera_id:
        return jsonify({"success": False, "code": 404, "msg": "camera_id is required."}), 404

    try:
        # DB에서 grid 상태 조회
        select_query = """
        SELECT camera_id, grid_data, sort_direction, grid_unit, point_list_data, set_mode, extend_count
        FROM tb_camera_safety_grid
        WHERE camera_id = %s
        """
        cursor.execute(select_query, (camera_id,))
        result = cursor.fetchone()

        if result is None:

            return jsonify({"success": False, "code": 200, "msg": "No grid state found for the given camera_id."}), 200

        # 결과 처리
        try:
            grid_data = json.loads(result[1]) # grid_data (JSON 문자열을 리스트로 변환)
        except TypeError:
            grid_data = result[1]
        sort_direction = result[2]         # sort_direction
        grid_unit = result[3]              # grid_unit
        point_list_data = json.loads(result[4])    
        set_mode = result[5]  # 설정 모드
        extend_count = result[6]  # 확장 카운트

        # Decode the original image from Base64 and save it
        image = decode_base64_to_image(image_base64)


        # Draw grid on image
        try:
            result_image = draw_grid_on_image(image, grid_data)
        except Exception as e:
            print(e)
            result_image = draw_grid_on_image(image, convert_nested_coordinates(grid_data))

        _, buffer = cv2.imencode('.jpg', result_image)
        view_image_base64 = base64.b64encode(buffer).decode('utf-8')

         # Process grid_data into approx_list
        approx_list = []
        for row_idx, row in enumerate(grid_data):
            approx_row = []
            for col_idx, col in enumerate(row):
                if col is not None:
                    approx_row.append(np.array(col, dtype=np.int32))
                else:
                    approx_row.append(None)
            approx_list.append(approx_row)

        # 새로운 unique_id 생성
        unique_id = str(uuid.uuid4())

         #이미지 사이즈 전송
        image_data = base64.b64decode(image_base64)
        image_array = np.frombuffer(image_data, dtype=np.uint8)
        image = Image.open(io.BytesIO(image_array))
        width, height = image.size
        # height와 width의 최대공약수(GCD) 구하기
        gcd = math.gcd(width, height)
        # 비율 계산
        width_ratio = width // gcd
        height_ratio = height // gcd

        # 수동 + 자동 모드에서만 생성
        if set_mode == '1':
            # loc_states에 상태 저장 (unique_id를 키로 사용)
            loc_states[unique_id] = {
                "camera_id": camera_id,
                "initial_coordinates": grid_data,
                "sort_direction": sort_direction,
                "approx_list": approx_list,
                "image_base64": image_base64,
                "origin_image_base64": image_base64,
                "grid_unit": grid_unit,
                "extend_count": extend_count,  # extend_count 복원
                "last_accessed": datetime.now(),
                "updated": False
            }

        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "camera_id": camera_id,
            "sort_direction": sort_direction,
            "set_mode": set_mode,
            "grid_unit": grid_unit,
            "unique_id": unique_id,
            "point_list_data": point_list_data, 
            "image_base64": view_image_base64,
        }), 200

    except KeyError as e:
        # Handle missing keys in the data
        return jsonify({"success": False, "code": 404, "msg": f"Missing key: {str(e)}"}), 404

    except ValueError as e:
        # Handle invalid input data
        return jsonify({"success": False, "code": 404, "msg": f"Invalid input: {str(e)}"}), 404

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        return jsonify({"success": False, "code": 500, "msg": "An unexpected error occurred.", "details": str(e)}), 500

    finally:
        if conn:
            cursor.close()
            conn.close()
        print("Closed to PostgreSQL successfully.")