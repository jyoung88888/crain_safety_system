from flask import Blueprint, request, jsonify
from blueprints.lib.public_func import fn_get_work_space, fn_upsert_work_space_cell, fn_get_work_space_cell,  fn_list_work_space_cell_detail, fn_get_work_space_cell_detail, fn_update_work_space_cell_detail, \
                                        fn_list_filled_work_space_cell_detail

# cctv 정보
# 블루프린트 생성, /cctv/work_space 상위 경로 추가
work_space = Blueprint('work_space', __name__, url_prefix='/cctv/work_space')

# cctv 데이터 추가
@work_space.route('/get_work_space', methods=['GET'])
def get_work_space():
    """
    작업공간 전체 조회 API
    - 예시 호출: /get_work_space
    """
    data = fn_get_work_space()
    return jsonify(data)



# ---------------------------------------------
# REST API 엔드포인트
# ---------------------------------------------
@work_space.route('/get_work_space_cell_detail/<work_space_cd>', methods=['GET'])
def get_work_space_cell_detail(work_space_cd):
    """
    특정 작업공간 코드에 대한 셀 상세 리스트 조회
    예: GET /get_work_space_cell_detail/AF
    반환: [{cell_cd, work_space_nm, product_nm}, ...]
    """
    work_space_cd = (work_space_cd or "").strip().upper()
    if not work_space_cd or len(work_space_cd) != 2:
        return jsonify({"error": "유효한 work_space_cd(2자리)가 필요합니다."}), 400

    rows, err = fn_list_work_space_cell_detail(work_space_cd)
    if err:
        return jsonify({"error": err}), 500

    # 존재하지 않아도 200 + 빈 리스트 반환 (리스트 API 관례)
    return jsonify(rows), 200


# ---------------------------------------------
# REST API 엔드포인트 (UPSERT)
# ---------------------------------------------
@work_space.route('/work_space_cell', methods=['POST'])
def upsert_work_space_cell():
    """
    요청 JSON 예시:
    {
      "work_space_cd": "AF",
      "cell_count_x": 12,
      "cell_count_y": 8
    }
    """
    try:
        body = request.get_json(force=True) or {}
        work_space_cd = (body.get("work_space_cd") or "").strip()
        cell_x = body.get("cell_count_x")
        cell_y = body.get("cell_count_y")

        # --- 입력값 검증 ---
        if not work_space_cd or len(work_space_cd) != 2:
            return jsonify({"error": "work_space_cd(길이 2)가 필요합니다."}), 400
        try:
            cell_x = int(cell_x)
            cell_y = int(cell_y)
        except (TypeError, ValueError):
            return jsonify({"error": "cell_count_x, cell_count_y는 정수여야 합니다."}), 400
        if cell_x <= 0 or cell_y <= 0:
            return jsonify({"error": "cell_count_x, cell_count_y는 1 이상이어야 합니다."}), 400

        # DB 처리
        row, err = fn_upsert_work_space_cell(work_space_cd, cell_x, cell_y)
        if err:
            return jsonify({"error": err}), 500
        return jsonify(row), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500



# ---------------------------------------------
# REST API 엔드포인트
# ---------------------------------------------
@work_space.route('/get_work_space_cell/<work_space_cd>', methods=['GET'])
def get_work_space_cell(work_space_cd):
    """
    작업공간 코드(work_space_cd)에 해당하는 1개 셀정보 조회 API
    예시: GET /get_work_space_cell/AF
    """
    work_space_cd = work_space_cd.strip().upper()
    if not work_space_cd or len(work_space_cd) != 2:
        return jsonify({"error": "유효한 work_space_cd(2자리)가 필요합니다."}), 400

    row, err = fn_get_work_space_cell(work_space_cd)
    if err:
        return jsonify({"error": err}), 500
    if not row:
        return jsonify({"error": f"{work_space_cd} 데이터가 존재하지 않습니다."}), 404

    return jsonify(row), 200


# ---------------------------------------------
# REST API 엔드포인트
# ---------------------------------------------
@work_space.route('/get_work_space_cell_detail_by_cd/<cell_cd>', methods=['GET'])
def get_work_space_cell_detail_by_cd(cell_cd):
    """
    특정 셀 코드(cell_cd)에 대한 상세정보 조회
    예: GET /get_work_space_cell_detail_by_cd/AF11
    """
    cell_cd = (cell_cd or "").strip().upper()
    if not cell_cd:
        return jsonify({"error": "유효한 cell_cd가 필요합니다."}), 400

    row, err = fn_get_work_space_cell_detail(cell_cd)
    if err:
        return jsonify({"error": err}), 500
    if not row:
        return jsonify({"error": f"{cell_cd} 데이터가 존재하지 않습니다."}), 404

    return jsonify(row), 200

# ---------------------------------------------
# REST API 엔드포인트 (PATCH)
# ---------------------------------------------
@work_space.route('/work_space_cell_detail/<cell_cd>', methods=['PATCH'])
def update_work_space_cell_detail(cell_cd):
    """
    특정 cell_cd의 셀 상세정보 부분 업데이트
    - URL: PATCH /work-space-cell-detail/<cell_cd>
    - Body(JSON): { "work_space_nm": "...", "product_nm": "...", "remark": "..." } (선택)
    - 예: PATCH /work-space-cell-detail/AF11
    """
    try:
        body = request.get_json(force=True) or {}
        work_space_nm = body.get("work_space_nm", None)
        product_nm    = body.get("product_nm", None)
        remark        = body.get("remark", None)

        cell_cd = (cell_cd or "").strip().upper()
        if not cell_cd:
            return jsonify({"error": "유효한 cell_cd가 필요합니다."}), 400

        row, err = fn_update_work_space_cell_detail(
            cell_cd=cell_cd,
            work_space_nm=work_space_nm,
            product_nm=product_nm,
            remark=remark
        )

        if err == "업데이트할 필드가 없습니다.":
            return jsonify({"error": err}), 400
        if err == "해당 cell_cd 데이터가 없습니다.":
            return jsonify({"error": err}), 404
        if err:
            return jsonify({"error": err}), 500

        return jsonify(row), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    


    # ---------------------------------------------
# REST API 엔드포인트
# ---------------------------------------------
@work_space.route('/get_work_space_cell_detail_filled', methods=['GET'])
def get_work_space_cell_detail_filled():
    """
    작업장명 또는 제품명이 입력된 모든 셀 상세 정보 조회
    예: GET /get_work_space_cell_detail_filled
    """
    rows, err = fn_list_filled_work_space_cell_detail()
    if err:
        return jsonify({"error": err}), 500

    return jsonify(rows), 200