from flask import Blueprint, request, jsonify
import requests
import json
from blueprints.lib.public_func import update_camera_roi, load_data, save_data, get_camera_roi_by_id, \
    get_all_camera_rois, get_ai_server_by_comp_id, get_camera_roi_by_comp_id, delete_camera_roi, CCTV_DATA_FILE, \
    get_camera_by_id, get_ai_server, upsert_camera_roi

# ROI 마스터
# 블루프린트 생성, /cctv/roi_crud 상위 경로 추가
cctv_roi = Blueprint('cctv_roi_crud', __name__, url_prefix='/cctv/roi_crud')
ROI_DATA_FILE = '../data/roi_data.json'

# ROI 데이터 추가
@cctv_roi.route('/roi', methods=['POST'])
def roi():
    # data = load_data(ROI_DATA_FILE)
    # new_id = int(max(data.keys(), default=0)) + 1
    # roi_data = request.json
    # data[new_id] = roi_data
    # save_data(data, ROI_DATA_FILE)
    roi_data = request.json
    print(roi_data, flush=True)

    camera_id = roi_data.get("cctv_id")
    model_nm  = roi_data.get("modelNm")
    is_run    = roi_data.get("is_run", True)
    user_cd   = roi_data.get("userCd", "")
    roi_list  = roi_data.get("roiList", []) 

    if not camera_id or not model_nm:
        return jsonify({"success": False, "code": 400, "msg": "cctv_id/modelNm 누락"}), 400

    created_by = user_cd.split("_", 1)[1] if "_" in user_cd else user_cd

    update_count = 0
    update_arr = []

    for row in roi_list:
        roi_id = str(row.get("roi_id"))
        polygons = row.get("point", [])  # [ [ [x,y],... ], [ [x,y],... ] ]

        # varchar 저장이므로 JSON 문자열로 직렬화
        point_str = json.dumps(polygons, ensure_ascii=False)

        ok = upsert_camera_roi(camera_id, model_nm, roi_id, point_str, created_by, is_run)
        if ok:
            update_count += 1
            update_arr.append([camera_id, model_nm, roi_id])

    return jsonify({
        "success": update_count > 0,
        "code": 200 if update_count > 0 else 404,
        "msg": "성공하였습니다." if update_count > 0 else "roi save fail",
        "update_count": update_count,
        "update_arr": update_arr
    })
    
    # print(roi_data)
    # update_count = 0
    # update_arr = []
    # for roi in roi_data:
        # print(roi['cctv_id'])
        # print(roi['model_list'])
        # 수정코드
        # for model in roi['model_list']:
        #     result = update_camera_roi(roi['cctv_id'], model['model_nm'],
        #                         model['point_arr'],
        #                         model['userCd'].split('_', 1)[1], model["is_run"])
        #     if result != None:
        #         update_arr.append([roi['cctv_id'], model['model_nm']])
        #         update_count += 1
    # if update_count != 0:
    #     return jsonify({
    #         "success": True,
    #         "code": 200,
    #         "msg": "성공하였습니다.",
    #         "message": "roi added successfully",
    #         "update_count": update_count, "update_arr": update_arr})
    # else:
    #     return jsonify({"success": False,
    #                     "code": 404,
    #                     "msg": "roi added fail"})


# ROI 데이터 수정
@cctv_roi.route('/roi/<int:roi_id>', methods=['PUT'])
def update_roi(roi_id):
    data = load_data(ROI_DATA_FILE)
    if str(roi_id) not in data:
        return jsonify({"message": "roi not found"})
    roi_data = request.json
    data[str(roi_id)] = roi_data
    print(roi_data)
    save_data(data, ROI_DATA_FILE)
    return jsonify({"message": "roi updated successfully"})


# ROI 데이터 조회 (특정 아이템 조회)
@cctv_roi.route('/roi/<string:roi_id>', methods=['GET'])
def get_roi(roi_id):
    # data = load_data(ROI_DATA_FILE)
    # model = data.get(str(roi_id))
    # if not model:
    #     return jsonify({"message": "roi not found"}), 404

    #  # Transforming the input data to the desired output format
    # output_data = {
    #     "data": model
    # }

    roi = get_camera_roi_by_id(roi_id.split('_')[0], roi_id.split('_')[1])
    if not roi:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "roi not found"})

    # Transforming the input data to the desired output format
    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "data": {
            "model_nm": roi[2],
            "cctv_id": roi[0],
            "point_arr": roi[1].replace('{', '[').replace('}', ']'),
            "is_run": roi[7],
            "created_at": roi[3].strftime("%Y.%m.%d %H:%M:%S") if roi[3] != None else None,
            "created_by": roi[4],
            "updated_at": roi[5].strftime("%Y.%m.%d %H:%M:%S") if roi[6] != None else None,
            "updated_by": roi[6]
        }
    }
    return jsonify(output_data)


# ROI 전체 조회
@cctv_roi.route('/rois', methods=['GET'])
def get_all_roi():
    # data = load_data(ROI_DATA_FILE)
    # # Transforming the input data to the desired output format
    # output_data = {
    #     "list": [
    #         {
    #             "model_nm": "Detection",
    #             "cctv_id": "CAM0002",
    #             "point_arr": [ [ 244, 473 ], [ 393, 300 ], [ 645, 290 ], [ 671, 331 ], [ 624, 484 ], [ 251, 470 ], [ 244, 473 ] ],
    #             "is_run": false,
    #             "userCd":"IGNS_esg_team",
    #             "__created__": true
    #         } for key, value in data.items()
    #     ]
    # }
    data =  ()
    # cctv_id list 저장
    cctv_id_lst = []

    for roi in data:
        if not roi[0] in cctv_id_lst:
            cctv_id_lst.append(roi[0])

    # print(cctv_id_lst)

    output_data = {
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": [
            {
                "cctv_id": cctv_id,
                "model_list": [
                    {
                        "model_nm": roi[2],
                        "point_arr": roi[1].replace('{', '[').replace('}', ']'),
                        "is_run": roi[7],
                        "created_at": roi[3].strftime("%Y.%m.%d %H:%M:%S") if roi[3] != None else None,
                        "created_by": roi[4],
                        "updated_at": roi[5].strftime("%Y.%m.%d %H:%M:%S") if roi[6] != None else None,
                        "updated_by": roi[6]
                    } for roi in data
                    if roi[0] == cctv_id
                ]
            } for cctv_id in cctv_id_lst
        ]
    }

    return jsonify(output_data)


BASE_MODELS = ["Detection", "Pose"] 

def safe_json(point_str):
    if not point_str:
        return []
    try:
        return json.loads(point_str)
    except:
        return []
# ROI 전체 조회
@cctv_roi.route('/rois/<string:userCd>', methods=['GET'])
def get_all_roi1(userCd):
    # data = load_data(ROI_DATA_FILE)
    # # Transforming the input data to the desired output format
    # output_data = {
    #     "list": [
    #         {
    #             "model_nm": "Detection",
    #             "cctv_id": "CAM0002",
    #             "point_arr": [ [ 244, 473 ], [ 393, 300 ], [ 645, 290 ], [ 671, 331 ], [ 624, 484 ], [ 251, 470 ], [ 244, 473 ] ],
    #             "is_run": false,
    #             "userCd":"IGNS_esg_team",
    #             "__created__": true
    #         } for key, value in data.items()
    #     ]
    # }
    parts = userCd.split('_', 1)
    comp_id = parts[0]

    data = get_all_camera_rois() if comp_id == "IGNS" else get_camera_roi_by_comp_id(comp_id)

    cam_map = {}  # camera_id -> camera obj

    for row in data:
        camera_id, camera_nm, camera_desc, point, model_nm, roi_id, is_run, created_at, created_by, updated_at, updated_by = row

        cam = cam_map.setdefault(camera_id, {
            "cctv_id": camera_id,
            "camera_nm": camera_nm,
            "camera_desc": camera_desc,
            # 모델 기본값(ROI 없어도 카메라가 내려가게)
            "model_list": { m: {"model_nm": m, "is_run": False, "roiList": []} for m in BASE_MODELS }
        })

        # ROI가 없는 카메라는 여기서 model_nm이 None이므로 skip
        if not model_nm:
            continue

        # base_models에 없는 모델이면 동적으로 추가
        if model_nm not in cam["model_list"]:
            cam["model_list"][model_nm] = {"model_nm": model_nm, "is_run": False, "roiList": []}

        model_obj = cam["model_list"][model_nm]
        model_obj["is_run"] = bool(is_run)

        model_obj["roiList"].append({
            "roi_id": str(roi_id),
            "point": safe_json(point),
            "created_at": created_at.strftime("%Y.%m.%d %H:%M:%S") if created_at else None,
            "created_by": created_by,
            "updated_at": updated_at.strftime("%Y.%m.%d %H:%M:%S") if updated_at else None,
            "updated_by": updated_by
        })

    # dict -> list 변환
    out_list = []
    for cam in cam_map.values():
        cam["model_list"] = list(cam["model_list"].values())
        out_list.append(cam)

    return jsonify({
        "success": True,
        "code": 200,
        "msg": "성공하였습니다.",
        "list": out_list
    })


# 모델 데이터 삭제
@cctv_roi.route('/roi', methods=['DELETE'])
def delete_roi():
    # data = load_data(ROI_DATA_FILE)
    # if str(roi_id) not in data:
    #     return jsonify({"message": "roi not found"}), 404
    # del data[str(roi_id)]
    # save_data(data, ROI_DATA_FILE)
    roi_data = request.json
    count = 0
    del_lst = []
    for roi_id in roi_data:
        result = delete_camera_roi(roi_id.split('_')[0], roi_id.split('_')[1])
        if result != False:
            del_lst.append([roi_id.split('_')[0], roi_id.split('_')[1]])
            count += 1
    if count != 0:
        return jsonify({
            "success": True,
            "code": 200,
            "msg": "성공하였습니다.",
            "message": f"roi deleted successfully", "count": count, "del_lst": del_lst})
    else:
        return jsonify({"success": False,
                        "code": 404,
                        "msg": "cctv not found"}), 404


# cctv 이미지 반환
@cctv_roi.route('/get_cctv_img/<string:cctv_id>', methods=['GET'])
def get_cctv_img(cctv_id):
    # cctv 이름
    # print(cctv_name)
    out_path = ''  # 경로
    server_id = ''  # 서버id

    cctv_data = get_camera_by_id(cctv_id)
    print(cctv_data)
    server_id = cctv_data[4]
    server_data = get_ai_server(server_id)
    server_ip = server_data[3]
    restapi_port = server_data[4]
    out_path = cctv_data[6]
    pid = cctv_data[7]

    # out_path =  cctv_data[cctv_id]['out_path']
    # server_id = cctv_data[cctv_id]['server_id']
    # for key in cctv_data.keys():
    #     # print(key)
    #     cctv = cctv_data[key]
    #     # print(cctv['cctv_name'])
    #     #cctv 검색
    #     if(cctv['cctv_name'] == cctv_name):
    #         out_path = cctv['out_path']
    #         server_id = cctv['server_id']
    #         break
    # 경로명
    # print("OUTPATH", out_path, flush=True)
    # 경로명
    # print("SERVERID", server_id, flush=True)
    # 카메라 데이터
    print("CCTVDATA", cctv_data, flush=True)

    # POST 요청을 보낼 URL
    # url = 'http://' + server_ip + ':' + restapi_port + '/cctv/process/get_img_socket'
    url = f'http://{server_ip}:{restapi_port}/cctv/process/get_img_socket'
    print(url, flush=True)

    # 요청에 포함할 데이터 (JSON 형식)
    data = {
        "out_path": out_path,
        "pid": pid,
        "cctv_id": cctv_id
    }

    # 헤더 (옵션, 필요 시 설정)
    headers = {
        'Content-Type': 'application/json'
    }

    # POST 요청 보내기
    # data = {"out_path": out_path, "pid": pid, "cctv_id": cctv_id}
    response = requests.post(url, data=json.dumps(data), headers=headers)
    # print('response')
    # print(response, flush=True)
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
        # print("상태 코드:", response.code)
        print("응답 내용:", response.text)

    return jsonify({"success": False,
                    "code": 404,
                    "msg": f"err"})
