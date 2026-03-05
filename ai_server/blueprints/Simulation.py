from flask import Blueprint, request, jsonify
import pandas as pd
from datetime import datetime

from blueprints.lib.public_func import sim_run_df, update_sim_master, get_sim_master_between, fn_get_sim_result, fn_get_sim_input_bwts, \
                                        fn_get_run_sim_input, fn_get_sim_input_scrubber, fn_get_work_space_cell_detail


# 서버 정보
# 블루프린트 생성, /cctv/sim 상위 경로 추가
cctv_sim = Blueprint('cctv_sim', __name__, url_prefix='/cctv/sim')


# 서버 데이터 추가 - free_mac과 relaxed_time은 빈 값으로 생성
@cctv_sim.route('/sim_run', methods=['POST'])
def sim_run():
    server_data = request.json

    created_at = server_data['created_at']
    created_by = server_data['created_by']
    job_list = server_data['job_list']
    machine_list = server_data['machine_list']

    result = sim_run_df(job_list, machine_list, created_at, created_by)

    # print(result)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "status": result['status'],
        "sim_id": result['sim_id']
    })

# sim_id를 받아 free_mac과 relaxed_time 업데이트
@cctv_sim.route('/update_sim', methods=['POST'])
def update_sim():
    server_data = request.json

    sim_id = server_data['sim_id']
    free_mac = server_data['free_mac']
    relaxed_time = server_data['relaxed_time']

    result = update_sim_master(sim_id, free_mac, relaxed_time)

    if result:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "시뮬레이션 정보가 업데이트되d었습니다.",
            "sim_id": sim_id
        })
    else:
        return jsonify({
            "success": False,
            "code": 404,
            "msg": "시뮬레이션 정보 업데이트에 실패했습니다.",
            "sim_id": sim_id
        }), 404

# 시뮬레이션 LIST
@cctv_sim.route('/get_sim_master_range', methods=['GET'])
def get_sim_master_range():
    start_day = request.args.get('start_day')
    end_day = request.args.get('end_day')
    try:
        type_code = request.args.get('type_code')
        if type_code == '' or type_code == None:
            type_code = 'BWTS'
    except Exception as e:
        print(e)
        type_code = 'BWTS'

    if not start_day or not end_day:
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "start_day, end_day 파라미터가 필요합니다."
        }), 400

    data = get_sim_master_between(start_day, end_day, type_code)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": data
    })

# 시뮬레이션 결과 반환 칸트차트 이미지
@cctv_sim.route('/get_sim_result', methods=['GET'])
def get_sim_result():
    sim_id = request.args.get('sim_id')
    base_time = request.args.get('base_time')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    base64_img = fn_get_sim_result(sim_id, base_time, start_date, end_date)

    # if not sim_id or not base_time:
    #     return jsonify({'error': 'sim_id, base_time 파라미터가 필요합니다.'}), 400

    if not sim_id:
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "sim_id 파라미터가 필요합니다."
        }), 400

    # data = get_sim_master_between(start_day, end_day)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": {
            'base64_img': base64_img
        }
    })



# 실행시뮬레이션 입력 데이터
@cctv_sim.route('/get_run_sim_input', methods=['GET'])
def get_run_sim_input():
    sim_id = request.args.get('sim_id')

    if not sim_id :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "sim_id 파라미터가 필요합니다."
        }), 400

    data = fn_get_run_sim_input(sim_id)

    # data = get_sim_master_between(start_day, end_day)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": data
    })


# 시뮬레이션 입력 데이터(수주현황) 
@cctv_sim.route('/get_sim_input_bwts', methods=['GET'])
def get_sim_input_bwts():
    base_time = request.args.get('base_time')

    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400

    data = fn_get_sim_input_bwts(base_time)

    # data = get_sim_master_between(start_day, end_day)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": data
    })


# 시뮬레이션 입력 데이터(수주현황) 
@cctv_sim.route('/get_sim_input_scrubber', methods=['GET'])
def get_sim_input_scrubber():
    base_time = request.args.get('base_time')

    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400

    data = fn_get_sim_input_scrubber(base_time)

    # data = get_sim_master_between(start_day, end_day)
    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": data
    })


# 시뮬레이션 진행률 조회 API는 /cctv/dt_model/dt_modeling/ 경로로 이동되었습니다.

# BWTS 시뮬레이션 실행 API - get_sim_input_bwts와 sim_run을 합친 API
# 요청 형식:
# 1. URL 쿼리 파라미터로 전달:
#    ?base_time=2025-04-05 23:59
# 
# 2. 또는 JSON 바디로 전달:
# {
#   "base_time": "YYYY-MM-DD"  // 필수: 기준 날짜
# }
# 
# 응답 형식:
# {
#   "success": true,
#   "code": 200,
#   "msg": "BWTS 시뮬레이션이 성공적으로 실행되었습니다.",
#   "status": "test",
#   "sim_id": 123  // 생성된 시뮬레이션 ID
# }
@cctv_sim.route('/BWTS_sim_run_new', methods=['POST'])
def BWTS_sim_run_new():
    # 요청에서 base_time 가져오기 (URL 쿼리 파라미터 우선, 없으면 JSON 바디에서 가져옴)
    base_time = request.args.get('base_time')
    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400
    try:
        processing_times = request.args.get('processing_times')
    except Exception as e:
        print(e)
        processing_times = 36

    try:
        relaxed_times = request.args.get('relaxed_times')
    except Exception as e:
        print(e)
        relaxed_times = 18

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 요청 바디에서 데이터 가져오기
    server_data = request.json or {}
    created_by = server_data.get('created_by')

    # 제외 목록 가져오기
    try:
        block_ids = server_data.get('block_ids')
    except Exception as e:
        print(e)
        block_ids = None

    # 작업공간 정보 가져오기
    try:
        work_space_info_list = server_data.get('work_space_info')
    except Exception as e:
        print(e)
        work_space_info_list = []

    work_space_info_dic = {item["grid_point"]: item["work_space"] for item in work_space_info_list}

    # 바디에 created_by가 없으면 URL 파라미터에서 가져오기 (하위 호환성 유지)
    if created_by is None:
        created_by = request.args.get('created_by')
    #입력 데이터(수주데이터)
    data = fn_get_sim_input_bwts(base_time, block_ids)

    # print(data)

    #작업장 데이터
    data1 = fn_get_work_space_cell_detail('BWTS')

    #작업장 할당 list
    set_work_space_list = []

    for idx, item in enumerate(data):
        if item.get('상세위치') != None:
            set_work_space_list.append(work_space_info_dic[item.get('상세위치')])

    no_set_work_space_list = []

    for idx, item in enumerate(data1):
        if item['work_space_nm'] not in set_work_space_list:
            no_set_work_space_list.append(item['work_space_nm'])
    
    job_list = []
    for idx, item in enumerate(data):
        # 수주번호와 순번을 기준으로 호선번호(hullno) 조회
        ordnum = item.get('수주번호')
        # if ordnum is None:
        #     ordnum = "none"
        ordseq = item.get('순번')
        # if ordseq is None:
        #     ordseq = "none"
        hullno = item.get('호선')
        # if hullno is None:
        #     hullno = "none"
        dlvdt = item.get('납기일자')
        # if dlvdt is None:
        #     dlvdt = "none"

        if item.get('상세위치') is None:
            try:
                detail_location = no_set_work_space_list.pop(0)
            except Exception as e:
                print(e)
                detail_location = None
        else:
            detail_location = work_space_info_dic[item.get('상세위치')]

        rate = item.get('작업진행율')
        # If progress rate is null, set it to 0%
        if rate is None:
            rate = "0"
        # If progress rate has % at the end, extract only the numeric part
        elif isinstance(rate, str) and "%" in rate:
            rate = rate.replace("%", "").strip()
        location = item.get('위치')
        if location is None:
            location = "none"
        enddt = item.get('종료일자')
        # if enddt is None:
        #     enddt = "none"

        job_list.append({
            "순번": idx + 1,
            "납기일": dlvdt,
            "호선명": hullno,  # 호선번호 추가
            "작업장": detail_location,
            "작업시간": processing_times,  
            "여유시간": relaxed_times,
            "작업진행률(%)":rate,
            "수주번호":ordnum,
            "수주순번":ordseq,
            "위치":location,
            "종료일자":enddt
        })

    machine_list = []
    for idx, item in enumerate(job_list):
        work_area = item.get('작업장')
        if work_area is None:
            continue

        machine_list.append({
            "순번": idx + 1,
            "작업장":work_area
        })

    #잔여 공간 추가
    for idx, item in enumerate(no_set_work_space_list):
        machine_list.append({
            "순번": len(machine_list) + idx,
            "작업장": item
        })

    # print(job_list)
    # print(machine_list)
    # 시뮬레이션 실행
    result = sim_run_df(job_list, machine_list, created_at, created_by, 'BWTS')

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        #"status": result['status'],
        "job_list": job_list,
        "machine_list": machine_list,
        "created_at": created_at,
        "created_by": created_by

    })

# 시뮬레이션 진행률 조회 API는 /cctv/dt_model/dt_modeling/ 경로로 이동되었습니다.

# BWTS 시뮬레이션 실행 API - get_sim_input_bwts와 sim_run을 합친 API
# 요청 형식:
# 1. URL 쿼리 파라미터로 전달:
#    ?base_time=2025-04-05 23:59
# 
# 2. 또는 JSON 바디로 전달:
# {
#   "base_time": "YYYY-MM-DD"  // 필수: 기준 날짜
# }
# 
# 응답 형식:
# {
#   "success": true,
#   "code": 200,
#   "msg": "BWTS 시뮬레이션이 성공적으로 실행되었습니다.",
#   "status": "test",
#   "sim_id": 123  // 생성된 시뮬레이션 ID
# }
@cctv_sim.route('/BWTS_sim_run', methods=['POST'])
def BWTS_sim_run():
    # 요청에서 base_time 가져오기 (URL 쿼리 파라미터 우선, 없으면 JSON 바디에서 가져옴)
    base_time = request.args.get('base_time')
    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400
    try:
        processing_times = request.args.get('processing_times')
    except Exception as e:
        print(e)
        processing_times = 36

    try:
        relaxed_times = request.args.get('relaxed_times')
    except Exception as e:
        print(e)
        relaxed_times = 18

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 요청 바디에서 데이터 가져오기
    server_data = request.json or {}
    created_by = server_data.get('created_by')

    # 제외 목록 가져오기
    try:
        block_ids = server_data.get('block_ids')
    except Exception as e:
        print(e)
        block_ids = None

    # 바디에 created_by가 없으면 URL 파라미터에서 가져오기 (하위 호환성 유지)
    if created_by is None:
        created_by = request.args.get('created_by')

    data = fn_get_sim_input_bwts(base_time, block_ids)
    print(data)
    job_list = []
    for idx, item in enumerate(data):
        # 수주번호와 순번을 기준으로 호선번호(hullno) 조회
        ordnum = item.get('수주번호')
        # if ordnum is None:
        #     ordnum = "none"
        ordseq = item.get('순번')
        # if ordseq is None:
        #     ordseq = "none"
        hullno = item.get('호선')
        # if hullno is None:
        #     hullno = "none"
        dlvdt = item.get('납기일자')
        # if dlvdt is None:
        #     dlvdt = "none"
        detail_location = item.get('상세위치')
        rate = item.get('작업진행율')
        # If progress rate is null, set it to 0%
        if rate is None:
            rate = "0"
        # If progress rate has % at the end, extract only the numeric part
        elif isinstance(rate, str) and "%" in rate:
            rate = rate.replace("%", "").strip()
        location = item.get('위치')
        if location is None:
            location = "none"
        enddt = item.get('종료일자')
        # if enddt is None:
        #     enddt = "none"

        job_list.append({
            "순번": idx + 1,
            "납기일": dlvdt,
            "호선명": hullno,  # 호선번호 추가
            "작업장": detail_location,
            "작업시간": processing_times,  
            "여유시간": relaxed_times,
            "작업진행률(%)":rate,
            "수주번호":ordnum,
            "수주순번":ordseq,
            "위치":location,
            "종료일자":enddt
        })

    machine_list = []
    for idx, item in enumerate(data):
        work_area = item.get('상세위치')
        if work_area is None:
            continue

        machine_list.append({
            "순번": idx + 1,
            "작업장":work_area
        })

    print(job_list)
    print(machine_list)
    # 시뮬레이션 실행
    result = sim_run_df(job_list, machine_list, created_at, created_by, 'BWTS')

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        #"status": result['status'],
        "job_list": job_list,
        "machine_list": machine_list,
        "created_at": created_at,
        "created_by": created_by

    })


# SCURBBER 시뮬레이션 실행 API - get_sim_input_scruber와 sim_run을 합친 API
# 요청 형식:
# 1. URL 쿼리 파라미터로 전달:
#    ?base_time=2025-04-05 23:59
# 
# 2. 또는 JSON 바디로 전달:
# {
#   "base_time": "YYYY-MM-DD"  // 필수: 기준 날짜
# }
# 
# 응답 형식:
# {
#   "success": true,
#   "code": 200,
#   "msg": "BWTS 시뮬레이션이 성공적으로 실행되었습니다.",
#   "status": "test",
#   "sim_id": 123  // 생성된 시뮬레이션 ID
# }
@cctv_sim.route('/SCRUBBER_sim_run_new', methods=['POST'])
def SCRUBBER_sim_run_new():
    # 요청에서 base_time 가져오기 (URL 쿼리 파라미터 우선, 없으면 JSON 바디에서 가져옴)
    base_time = request.args.get('base_time')
    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400

    try:
        processing_times = request.args.get('processing_times')
    except Exception as e:
        print(e)
        processing_times = 18

    try:
        relaxed_times = request.args.get('relaxed_times')
    except Exception as e:
        print(e)
        relaxed_times = 18

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")



    # 요청 바디에서 데이터 가져오기
    server_data = request.json or {}
    created_by = server_data.get('created_by')

    # 제외 목록 가져오기
    try:
        block_ids = server_data.get('block_ids')
    except Exception as e:
        print(e)
        block_ids = None

    # 작업공간 정보 가져오기
    try:
        work_space_info_list = server_data.get('work_space_info')
    except Exception as e:
        print(e)
        work_space_info_list = []

    work_space_info_dic = {item["grid_point"]: item["work_space"] for item in work_space_info_list}

    # 바디에 created_by가 없으면 URL 파라미터에서 가져오기 (하위 호환성 유지)
    if created_by is None:
        created_by = request.args.get('created_by')
    
    #입력 데이터(수주데이터)
    data = fn_get_sim_input_scrubber(base_time, block_ids)

    print(data)

    #작업장 데이터
    data1 = fn_get_work_space_cell_detail('SCRUBBER')

    print(data1)

    #작업장 할당 list
    set_work_space_list = []

    for idx, item in enumerate(data):
        if item.get('상세위치') != None:
            set_work_space_list.append(work_space_info_dic[item.get('상세위치')])

    print(set_work_space_list)

    no_set_work_space_list = []

    for idx, item in enumerate(data1):
        if item['work_space_nm'] not in set_work_space_list:
            no_set_work_space_list.append(item['work_space_nm'])

    print(no_set_work_space_list)

    job_list = []
    for idx, item in enumerate(data):
        # 수주번호와 순번을 기준으로 호선번호(hullno) 조회
        ordnum = item.get('수주번호')
        # if ordnum is None:
        #     ordnum = "none"
        ordseq = item.get('순번')
        # if ordseq is None:
        #     ordseq = "none"
        hullno = item.get('호선')
        # if hullno is None:
        #     hullno = "none"
        dlvdt = item.get('납기일자')
        # if dlvdt is None:
        #     dlvdt = "none"

        if item.get('상세위치') is None:
            try:
                detail_location = no_set_work_space_list.pop(0)
            except Exception as e:
                detail_location = None
        else:
            detail_location = work_space_info_dic[item.get('상세위치')]

        rate = item.get('작업진행율')
        # If progress rate is null, set it to 0%
        if rate is None:
            rate = "0"
        # If progress rate has % at the end, extract only the numeric part
        elif isinstance(rate, str) and "%" in rate:
            rate = rate.replace("%", "").strip()
        location = item.get('위치')
        if location is None:
            location = "none"
        enddt = item.get('종료일자')
        # if enddt is None:
        #     enddt = "none"

        job_list.append({
            "순번": idx + 1,
            "납기일": dlvdt,
            "호선명": hullno,  # 호선번호 추가
            "작업장": detail_location,
            "작업시간": processing_times,  
            "여유시간": relaxed_times,
            "작업진행률(%)":rate,
            "수주번호":ordnum,
            "수주순번":ordseq,
            "위치":location,
            "종료일자":enddt
        })

    machine_list = []
    for idx, item in enumerate(job_list):
        work_area = item.get('작업장')
        if work_area is None:
            continue

        machine_list.append({
            "순번": idx + 1,
            "작업장":work_area
        })

    #잔여 공간 추가
    for idx, item in enumerate(no_set_work_space_list):
        machine_list.append({
            "순번": len(machine_list) + idx,
            "작업장": item
        })

    # print(job_list)
    # print(machine_list)
    # 시뮬레이션 실행
    result = sim_run_df(job_list, machine_list, created_at, created_by, 'SCRUBBER')

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        #"status": result['status'],
        "job_list": job_list,
        "machine_list": machine_list,
        "created_at": created_at,
        "created_by": created_by

    })


# SCURBBER 시뮬레이션 실행 API - get_sim_input_scruber와 sim_run을 합친 API
# 요청 형식:
# 1. URL 쿼리 파라미터로 전달:
#    ?base_time=2025-04-05 23:59
# 
# 2. 또는 JSON 바디로 전달:
# {
#   "base_time": "YYYY-MM-DD"  // 필수: 기준 날짜
# }
# 
# 응답 형식:
# {
#   "success": true,
#   "code": 200,
#   "msg": "BWTS 시뮬레이션이 성공적으로 실행되었습니다.",
#   "status": "test",
#   "sim_id": 123  // 생성된 시뮬레이션 ID
# }
@cctv_sim.route('/SCRUBBER_sim_run', methods=['POST'])
def SCRUBBER_sim_run():
    # 요청에서 base_time 가져오기 (URL 쿼리 파라미터 우선, 없으면 JSON 바디에서 가져옴)
    base_time = request.args.get('base_time')
    if not base_time :
        return jsonify({
            "success": False,
            "code": 400,
            "msg": "base_time 파라미터가 필요합니다."
        }), 400

    try:
        processing_times = request.args.get('processing_times')
    except Exception as e:
        print(e)
        processing_times = 18

    try:
        relaxed_times = request.args.get('relaxed_times')
    except Exception as e:
        print(e)
        relaxed_times = 18

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M")



    # 요청 바디에서 데이터 가져오기
    server_data = request.json or {}
    created_by = server_data.get('created_by')

    # 제외 목록 가져오기
    try:
        block_ids = server_data.get('block_ids')
    except Exception as e:
        print(e)
        block_ids = None

    # 바디에 created_by가 없으면 URL 파라미터에서 가져오기 (하위 호환성 유지)
    if created_by is None:
        created_by = request.args.get('created_by')

    data = fn_get_sim_input_scrubber(base_time, block_ids)
    print(data)
    job_list = []
    for idx, item in enumerate(data):
        # 수주번호와 순번을 기준으로 호선번호(hullno) 조회
        ordnum = item.get('수주번호')
        # if ordnum is None:
        #     ordnum = "none"
        ordseq = item.get('순번')
        # if ordseq is None:
        #     ordseq = "none"
        hullno = item.get('호선')
        # if hullno is None:
        #     hullno = "none"
        dlvdt = item.get('납기일자')
        # if dlvdt is None:
        #     dlvdt = "none"
        detail_location = item.get('상세위치')
        rate = item.get('작업진행율')
        # If progress rate is null, set it to 0%
        if rate is None:
            rate = "0"
        # If progress rate has % at the end, extract only the numeric part
        elif isinstance(rate, str) and "%" in rate:
            rate = rate.replace("%", "").strip()
        location = item.get('위치')
        if location is None:
            location = "none"
        enddt = item.get('종료일자')
        # if enddt is None:
        #     enddt = "none"

        job_list.append({
            "순번": idx + 1,
            "납기일": dlvdt,
            "호선명": hullno,  # 호선번호 추가
            "작업장": detail_location,
            "작업시간": processing_times,  
            "여유시간": relaxed_times,
            "작업진행률(%)":rate,
            "수주번호":ordnum,
            "수주순번":ordseq,
            "위치":location,
            "종료일자":enddt
        })

    machine_list = []
    for idx, item in enumerate(data):
        work_area = item.get('상세위치')
        if work_area is None:
            continue

        machine_list.append({
            "순번": idx + 1,
            "작업장":work_area
        })

    print(job_list)
    print(machine_list)
    # 시뮬레이션 실행
    result = sim_run_df(job_list, machine_list, created_at, created_by, 'SCRUBBER')

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        #"status": result['status'],
        "job_list": job_list,
        "machine_list": machine_list,
        "created_at": created_at,
        "created_by": created_by

    })
