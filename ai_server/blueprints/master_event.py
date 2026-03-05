from flask import Blueprint, request, jsonify, Response
import requests
import json
from blueprints.lib.public_func import update_camera_event_hist, update_camera_event_hist_remart, \
    get_camera_event_serch, get_camera_event_serch1, get_all_camera_event_hist_by_group, \
    get_all_camera_event_hist_group_list, get_camera_by_id, get_ai_server, \
    get_camera_event_count, get_all_camera_event_hist1_by_event_type, fn_get_unread_event_counts, \
    get_camera_event_alert_by_group, get_camera_history

# camera_event 마스터
# 블루프린트 생성, /cctv/roi_crud 상위 경로 추가
ce = Blueprint('camera_event_crud', __name__, url_prefix='/cctv/ce/')


@ce.route('/camera_unread_events', methods=['GET'])
def get_unread_events_api():
    """
    특정 카메라의 미확인 이벤트 카운트를 반환하는 API

    예)
      GET /camera_unread-events?camera_id=CAM0071
      GET /camera_unread-events?camera_id=CAM0071&event_date=2025-08-18

    응답 예)
    [
      {
        "event_type": "NO_HELMET",
        "event_desc": "안전모 미착용",
        "unread_count": 3
      },
      {
        "event_type": "NO_VEST",
        "event_desc": "조끼 미착용",
        "unread_count": 1
      }
    ]
    """
    camera_id = (request.args.get('camera_id') or "").strip()
    event_date = (request.args.get('event_date') or "").strip() or None  # 없으면 None

    if not camera_id:
        return jsonify({"error": "camera_id 파라미터가 필요합니다."}), 400

    rows, err = fn_get_unread_event_counts(camera_id, event_date)

    if err:
        return jsonify({"status": "error", "message": err}), 500

    # rows는 RealDictCursor 덕분에 이미 dict 리스트
    return jsonify(rows), 200

# camera_event 데이터 추가
@ce.route('/camera_event', methods=['POST'])
def camera_event():
    # data = load_data(ROI_DATA_FILE)
    # new_id = int(max(data.keys(), default=0)) + 1
    # roi_data = request.json
    # data[new_id] = roi_data
    # save_data(data, ROI_DATA_FILE)
    camera_event_data = request.json
    # print(roi_data)
    update_count = 0
    update_arr = []
    for event in camera_event_data:
        # print(roi['cctv_id'])
        # print(roi['model_list'])
        # 수정코드
        result = update_camera_event_hist(event['event_time'], event['cctv_id'],
                                          True)
        if result != None:
            update_arr.append([event['cctv_id'], event['event_time']])
            update_count += 1
    if update_count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "camera_event added & updateed successfully",
            "update_count": update_count, "update_arr": update_arr}), 201
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "camera_event added & updateed fail"}), 404


# camera_event 비고 입력

@ce.route('/camera_events_remark', methods=['POST'])
def set_remark():
    # 검색조건
    ce_data = request.json
    # 수정코드
    result = update_camera_event_hist_remart(ce_data['event_time'], ce_data['cctv_id'],
                                             ce_data['remark'])
    if result:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": "camera_event updateed successfully"}), 201
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "camera_event updateed fail"}), 404


# camera_event 데이터 전체 조회
@ce.route('/camera_events', methods=['POST'])
def get_camera_event():
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

    # 검색조건
    serch_data = request.json

    parts = serch_data['userCd'].split('_', 1)

    if parts[0] == 'IGNS':
        data = get_camera_event_serch(serch_data['start_date'], serch_data['end_date'],
                                      None, serch_data['cctv_id'], serch_data['event_type'])
    else:
        print(parts[0])
        # data = get_all_camera_event_hist_by_comp_id(parts[0])
        data = get_camera_event_serch(serch_data['start_date'], serch_data['end_date'],
                                      parts[0], serch_data['cctv_id'], serch_data['event_type'])

    # print(data)
    list = [
        {
            "event_time": event[0].strftime("%Y.%m.%d %H:%M:%S") if event[0] != None else None,
            "cctv_id": event[1],
            "event_type": event[2],
            "event_desc": event[3],
            "file_path": event[4],
            "isread": event[5],
            "remark": event[6] if event[0] != None else ''
        } for event in data
        if event != None
    ]
    # list =  sorted(list, key=lambda x: x['event_time'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# camera_event 데이터 전체 조회 (배열)
@ce.route('/camera_events1', methods=['POST'])
def get_camera_event1():
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

    # 검색조건
    serch_data = request.json

    parts = serch_data['userCd'].split('_', 1)

    if parts[0] == 'IGNS':
        data = get_camera_event_serch1(serch_data['start_date'], serch_data['end_date'],
                                       None, serch_data['cctv_id'], serch_data['event_type'])
    else:
        print(parts[0])
        # data = get_all_camera_event_hist_by_comp_id(parts[0])
        data = get_camera_event_serch1(serch_data['start_date'], serch_data['end_date'],
                                       parts[0], serch_data['cctv_id'], serch_data['event_type'])

    # print(data)
    list = [
        {
            "event_time": event[0].strftime("%Y.%m.%d %H:%M:%S") if event[0] != None else None,
            "cctv_id": event[1],
            "event_type": event[2],
            "event_desc": event[3],
            "file_path": event[4],
            "isread": event[5],
            "remark": event[6] if event[0] != None else ''
        } for event in data
        if event != None
    ]
    # list =  sorted(list, key=lambda x: x['event_time'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# camera_event 데이터 전체 조회
@ce.route('/camera_events_by_group/<string:grp_id>', methods=['GET'])
def camera_events_by_group(grp_id):
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

    data = get_all_camera_event_hist_by_group(grp_id)

    list = [
        {
            "event_time": event[0].strftime("%Y.%m.%d %H:%M:%S") if event[0] != None else None,
            "cctv_id": event[1],
            "event_type": event[2],
            "event_desc": event[3],
            "file_path": event[4],
            "isread": event[5],
            "remark": event[6] if event[0] != None else ''
        } for event in data
        if event != None
    ]
    # list =  sorted(list, key=lambda x: x['event_time'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# camera_event 데이터 전체 조회
@ce.route('/camera_events_by_group1', methods=['POST'])
def camera_events_by_group1():
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
    serch_data = request.json

    print(serch_data)

    parts = serch_data['userCd'].split('_', 1)

    if parts[0] == 'IGNS':
        group_data = get_all_camera_event_hist_group_list(None)
    else:
        # print(parts[0])
        # data = get_all_camera_event_hist_by_comp_id(parts[0])
        group_data = get_all_camera_event_hist_group_list(parts[0])

    print(group_data)

    group_list = [
        {
            "profile_id": group[0],
            "isAlert": False if group[1] == None else True if group[1] > 0 else False
        } for group in group_data
    ]

    data = get_all_camera_event_hist_by_group(serch_data['profile_id'])

    list = [
        {
            "event_time": row[0].strftime("%Y.%m.%d %H:%M:%S") if row[0] else None,
            "event_type": row[1],
            "event_desc": row[2],
            "images": row[3] or [],          # [{camera_id,file_path},...]
            "isread": row[4],
            "remark": row[5] or ""
        }
        for row in data if row
    ]

    cam_rows = get_camera_event_alert_by_group(serch_data['profile_id'])
    camera = [
        {
            "camera_id": r[0],  # 또는 "cctv_id": r[0]
            "isAlert": True if (r[2] or 0) > 0 else False,   # unread_cnt 기준
            "unread_cnt": int(r[2] or 0),
            "total_cnt": int(r[1] or 0),
            "last_event_time": r[3].strftime("%Y.%m.%d %H:%M:%S") if r[3] else None
        }
        for r in cam_rows
    ]

    # list =  sorted(list, key=lambda x: x['event_time'])
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "group": group_list,
        "list": list,
        "camera": camera
    }

    return jsonify(output_data)
    
# camera_event 데이터 이번트 타입, 장소 상세 조회
@ce.route('/camera_events_by_event_type', methods=['GET'])
def camera_events_by_event_type():
    event_type = request.args.get('event_type')

    if not event_type:
        return jsonify({"status": "error", "message": "Missing required parameter: event_type"}), 400

    location = request.args.get('location')
    if not location:
        # 전체 안전 실적 내용
        data = get_all_camera_event_hist1_by_event_type(event_type)
        # list =  sorted(list, key=lambda x: x['event_time'])
    else:
        data = get_all_camera_event_hist1_by_event_type(event_type, location)

    list = [
            {
                "event_time": event[0].strftime("%Y.%m.%d %H:%M:%S") if event[0] != None else None,
                "cctv_id": event[1],
                "location": event[2],
                "event_type": event[3],
                "event_desc": event[4],
                "file_path": event[5],
                "isread": event[6],
                "remark": event[7] if event[0] != None else ''
            } for event in data
            if event != None
        ]
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)

# camera_event 데이터 통계 조회
@ce.route('/camera_events_count', methods=['GET'])
def camera_events_count():
    location = request.args.get('location')

    if not location:
        # 전체 안전 실적 내용
        data = get_camera_event_count()
        # list =  sorted(list, key=lambda x: x['event_time'])
    else:
        data = get_camera_event_count(location)

    list = [
            {
                "event_type": event[0],
                "event_desc": event[1],
                "event_count": event[2],
                "alarm_check": event[3]
            } for event in data
            if event != None
        ]
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": list
    }

    return jsonify(output_data)


# camera_event 파일받기
@ce.route('/camera_events_get_file', methods=['POST'])
def camera_events_get_file():
    file_data = request.json

    # 서버 정보 찾기
    # CCTV_DATA 전체 로드
    cctv_data = get_camera_by_id(file_data['cctv_id'])
    # print(cctv_data)
    server_id = cctv_data[4]
    server_data = get_ai_server(server_id)
    server_ip = server_data[3]
    restapi_port = server_data[4]

    # POST 요청을 보낼 URL
    url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/get_file1'
    # print(url)
    # 요청에 포함할 데이터 (JSON 형식)
    data = {
        "file_path": file_data['file_path'],
        "cctv_id": file_data['cctv_id']
    }
    # print(data)
    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }
    # POST 요청 보내기
    response = requests.post(url, data=json.dumps(data), headers=headers)
    # 응답 처리
    if response.status_code == 200:
        print("POST 요청 성공")
        # print("응답 데이터:", response.json())
        # return jsonify({"message": f"stop successfully"})

        out_text = response.json()
        out_text["success"] = True
        out_text["code"] = 200
        out_text["msg"] = "성공하였습니다."
        return jsonify(out_text)
    else:
        print("POST 요청 실패")
        print("상태 코드:", response.status_code)
        print("응답 내용:", response.text)

    return jsonify({"success": True,
                    "code": 200,
                    "msg": f"성공하였습니다",
                    'img_decode_data': None,
                    'img_size': {
                        'width': None,
                        'height': None,
                    },
                    'ratio': None
                    })
    # # POST 요청 보내기
    # try:
    #     response = requests.post(url, data=json.dumps(data), headers=headers)
    #     print()
    #     send_response = Response(response.content)
    #     send_response.headers['Content-Disposition'] = response.headers['Content-Disposition']
    #     send_response.headers['Content-Type'] = response.headers['Content-Type']
    #     send_response.headers['Content-Length'] = response.headers['Content-Length']
    #     send_response.headers['Last-Modified'] = response.headers['Content-Disposition']
    #     send_response.headers['Cache-Control'] = response.headers['Last-Modified']
    #     send_response.headers['ETag'] = response.headers['ETag']
    #     send_response.headers['Connection'] =  response.headers['Connection']

    #     return send_response
    # except Exception as e:
    #     print(e)
    #     return None

    # camera_event 파일받기


@ce.route('/camera_events_get_file2', methods=['POST'])
def camera_events_get_file2():
    file_data = request.json

    # 서버 정보 찾기
    # CCTV_DATA 전체 로드
    cctv_data = get_camera_by_id(file_data['cctv_id'])
    # print(cctv_data)
    server_id = cctv_data[4]
    server_data = get_ai_server(server_id)
    server_ip = server_data[3]
    restapi_port = server_data[4]

    # POST 요청을 보낼 URL
    url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/get_file'
    # print(url)
    # 요청에 포함할 데이터 (JSON 형식)
    data = {
        "file_path": file_data['file_path'],
        "cctv_id": file_data['cctv_id']
    }
    # print(data)
    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }
    # POST 요청 보내기
    response = requests.post(url, data=json.dumps(data), headers=headers)
    # 응답 처리
    # if response.status_code == 200:
    #     print("POST 요청 성공")
    #     # print("응답 데이터:", response.json())
    #     # return jsonify({"message": f"stop successfully"})

    #     out_text = response.json()
    #     out_text["success"] = True
    #     out_text["code"] = 200
    #     out_text["msg"] = "성공하였습니다."
    #     return jsonify(out_text)
    # else:
    #     print("POST 요청 실패")
    #     print("상태 코드:", response.status_code)
    #     print("응답 내용:", response.text)

    # return jsonify({"success": True,
    #                     "code": 200,
    #                     "msg": f"성공하였습니다",
    #                     'img_decode_data': None,
    #                     'img_size': {
    #                         'width': None,
    #                         'height': None,
    #                     },
    #                     'ratio':  None
    #                     })
    # POST 요청 보내기
    try:
        response = requests.post(url, data=json.dumps(data), headers=headers)
        print()
        send_response = Response(response.content)
        send_response.headers['Content-Disposition'] = response.headers['Content-Disposition']
        send_response.headers['Content-Type'] = response.headers['Content-Type']
        send_response.headers['Content-Length'] = response.headers['Content-Length']
        send_response.headers['Last-Modified'] = response.headers['Content-Disposition']
        send_response.headers['Cache-Control'] = response.headers['Last-Modified']
        send_response.headers['ETag'] = response.headers['ETag']
        send_response.headers['Connection'] = response.headers['Connection']

        return send_response
    except Exception as e:
        print(e)
        return None

@ce.route('/camera_hist', methods=['POST'])
def get_camera_hist():
    search_data = request.json

    parts = search_data['userCd'].split('_', 1)
    prefix = parts[0] if parts else ''

    comp_id = None if prefix == 'IGNS' else prefix

    data = get_camera_history(
        search_data.get('start_date'),
        search_data.get('end_date'),
        comp_id,
        search_data.get('event_type') or []
    )

    result = []
    for row in data:
        event_time, event_type, event_desc, images, isread, remark = row

        result.append({
            "event_time": event_time.strftime("%Y.%m.%d %H:%M:%S") if event_time else None,
            "event_type": event_type,
            "event_desc": event_desc,
            "images": images or [],
            "isread": isread,
            "remark": remark or ""
        })

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": result
    })