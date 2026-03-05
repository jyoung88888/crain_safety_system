from flask import Blueprint, request, jsonify

from blueprints.lib.public_func import fn_get_order_pro, fn_get_inven, get_detection_label_changes, get_detection_history, get_processing_time

# 서버 정보
# 블루프린트 생성, /cctv/monitor 상위 경로 추가
cctv_dt_monitor = Blueprint('cctv_monitor', __name__, url_prefix='/cctv/monitor')


# 수주진행현황 반환
@cctv_dt_monitor.route('/get_order_pro', methods=['GET'])
def get_order_pro():
    # start_day = request.args.get('start_day')
    # end_day = request.args.get('end_day')
    location = request.args.get('location')
    sim_id = request.args.get('sim_id')
    base_time = request.args.get('base_time')

    # if not start_day or not end_day:
    #     return jsonify({'error': 'start_day, end_day 파라미터가 필요합니다.'}), 400

    data = fn_get_order_pro(location, sim_id, base_time)

    return jsonify(data)


# 탐지 기록 조회
@cctv_dt_monitor.route('/get_detection_label_changes', methods=['GET'])
def get_detection_label_changes_api():
    """
    객체 라벨 변경 이력을 조회하는 API
    
    이 API는 특정 위치(카메라 ID, 행, 열)에서 객체 라벨이 변경된 이력을 조회합니다.
    각 변경에 대해 이전 라벨(previous_label), 현재 라벨(current_label), 시작 시간, 종료 시간, 실행 시간(초) 및 
    포맷된 실행 시간(시간, 분, 초)을 반환합니다.
    
    지정된 날짜 범위 내의 데이터만 조회하도록 start_date와 end_date 파라미터를 사용할 수 있습니다.
    
    Query Parameters:
        camera_id (str, optional): 선택 - 특정 카메라 ID로 필터링
        start_date (str, optional): 선택 - 시작 날짜 (YYYY-MM-DD 또는 YYYY-MM-DD HH:MM:SS 형식)
        end_date (str, optional): 선택 - 종료 날짜 (YYYY-MM-DD 또는 YYYY-MM-DD HH:MM:SS 형식)
    
    Returns:
        JSON: 객체 라벨 변경 이력 데이터
    """
    # 요청 파라미터 가져오기
    camera_id = request.args.get('camera_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # 라벨 변경 이력 조회
    result = get_detection_label_changes(camera_id, start_date, end_date)
    
    # 결과 반환
    return jsonify(result)

@cctv_dt_monitor.route('get_processing_time', methods=['GET'])
def get_processing_time_api():
    """
    공정진행별 평균 진행 시간을 반환

    Query Parameters:
        start_date (str, required): 필수 - 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str, required): 필수 - 종료 날짜 (YYYY-MM-DD 형식)

    Returns:
        JSON: 
            - data: 쿼리 결과 데이터(bwts대상, 0%, 10%, 30%, 70%, 90%, 100% 각 단계의 평균 시간)
    """
    # 요청 파라미터 가져오기
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

        # 필수 파라미터 검증
    if not start_date or not end_date:
        return jsonify({
            "status": "error",
            "message": "start_date와 end_date 파라미터가 필요합니다."
        }), 400
    
    # 탐지 기록 조회
    result = get_processing_time(start_date, end_date)
    
    # 결과 반환
    return jsonify(result)
    


@cctv_dt_monitor.route('/get_detection_history', methods=['GET'])
def get_detection_history_api():
    """
    탐지 기록 및 위치 변화를 조회하는 API
    
    이 API는 특정 위치(카메라 ID, 행, 열)에서 객체의 탐지 기록과 위치 변화를 조회합니다.
    각 위치에 대해 이전 라벨, 현재 라벨, 시작 시간, 종료 시간, 지속 시간(초) 및 변화 유형을 반환합니다.
    
    지정된 날짜 범위 내의 데이터만 조회하도록 start_date와 end_date 파라미터를 사용해야 합니다.
    특정 행(row)과 열(col)로 필터링할 수 있습니다.
    
    Query Parameters:
        start_date (str, required): 필수 - 시작 날짜 (YYYY-MM-DD 형식)
        end_date (str, required): 필수 - 종료 날짜 (YYYY-MM-DD 형식)
        object_label (str, optional): 선택 - 특정 객체 라벨로 필터링
        detected_row (int, optional): 선택 - 특정 행(row)으로 필터링
        detected_col (int, optional): 선택 - 특정 열(col)으로 필터링
    
    Returns:
        JSON: 다음 항목을 포함하는 탐지 기록 및 위치 변화 데이터
            - data: 쿼리 결과 데이터
            - average: 전체 평균 진행시간
            - camera_averages: 카메라별 평균 진행시간
            - row_col_averages: 행-열 쌍별 평균 진행시간
            - label_averages: 객체 라벨별 평균 진행시간
    """
    # 요청 파라미터 가져오기
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    object_label = request.args.get('object_label')
    detected_row = request.args.get('detected_row')
    detected_col = request.args.get('detected_col')
    
    # 필수 파라미터 검증
    if not start_date or not end_date:
        return jsonify({
            "status": "error",
            "message": "start_date와 end_date 파라미터가 필요합니다."
        }), 400
    
    # 정수형 파라미터 변환
    if detected_row is not None:
        try:
            detected_row = int(detected_row)
        except ValueError:
            return jsonify({
                "status": "error",
                "message": "detected_row 파라미터는 정수여야 합니다."
            }), 400
            
    if detected_col is not None:
        try:
            detected_col = int(detected_col)
        except ValueError:
            return jsonify({
                "status": "error",
                "message": "detected_col 파라미터는 정수여야 합니다."
            }), 400
    
    # 탐지 기록 조회
    result = get_detection_history(start_date, end_date, object_label, detected_row, detected_col)
    
    # 결과 반환
    return jsonify(result)


# 재고 현황 반환
@cctv_dt_monitor.route('/get_inven', methods=['GET'])
def get_inven():
    base_time = request.args.get('base_time')
    location = request.args.get('location')
    sim_id = request.args.get('sim_id')
    print(base_time)
    # start_day = request.args.get('start_day')
    # end_day = request.args.get('end_day')
    # location = request.args.get('location')
    # print(location)
    # if not start_day or not end_day:
    #     return jsonify({'error': 'start_day, end_day 파라미터가 필요합니다.'}), 400

    data = fn_get_inven(location, base_time, sim_id)

    return jsonify(data)



