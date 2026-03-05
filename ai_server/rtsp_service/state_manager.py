import os 
import sys
# subprocess로 실행 시 상위 디렉토리(/app)를 모듈 검색 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import redis
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import base64
from config.config import Config
from rtsp_service.lib.zone_utils import (
    aggregate_max_cross_zone,
    compute_safety,
    SafetyStateMachine,
    ZonePeopleStabilizer,
)
from rtsp_service.lib.telegram_alert import send_telegram_alert

from blueprints.cctv_alarm import set_alarm
from lib.public_func import insert_camera_event_hist, \
    create_directory_if_not_exists, image_save_dir

# ─────────────────────────────────────────────
# Redis 연결
# ─────────────────────────────────────────────

redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    decode_responses=True
)

TARGET_CLASSES = ["person_with_helmet", "person_no_helmet", "hoist"]
ALL_ZONES = [1,2]  # 1 : 1층 / 2 : 2층 
EMPTY_SLEEP_SEC = 0.1
LOOP_SLEEP_SEC = 0.2
STATUS_LOG_INTERVAL_SEC = 5.0
WARMUP_SEC = 10              # 시작 후 이벤트 억제 시간 (초)
MIN_STABLE_CYCLES = 5        # 최소 연속 정상 수신 횟수
EVAL_EVERY_N_FRAMES = 5      # 5프레임마다 한 번 판단

# ─────────────────────────────────────────────
# Redis에서 전체 카메라 상태 읽기
# ─────────────────────────────────────────────
def read_all_camera_states():
    zone_data = {}
    moving_data = {}
    frame_data = {}
    image_b64_data = {}

    try:
        for key in redis_client.scan_iter("cam:*:snapshot"):
            cid = key.split(":")[1]
            snapshot = redis_client.hgetall(key)
            if not snapshot:
                continue

            raw_counts = snapshot.get("zone_counts_json")
            if raw_counts:
                parsed = _normalize_zone_counts(raw_counts)
                if parsed is not None:
                    zone_data[cid] = parsed

            val = snapshot.get("hoist_moving")
            if val is not None:
                moving_data[cid] = (val == "1")

            val = snapshot.get("frame_id")
            if val is not None:
                try:
                    frame_data[cid] = int(val)
                except Exception:
                    pass

            raw_jpeg = snapshot.get("jpeg_b64")
            if raw_jpeg:
                image_b64_data[cid] = raw_jpeg

    except Exception as e:
        print(f"[StateManager] Redis READ ERROR: {e}", file=sys.stderr, flush=True)

    return zone_data, moving_data, frame_data, image_b64_data


def _decode_event_images(cam_ids, image_b64_data):
    images = {}
    for cam_id in cam_ids:
        raw_jpeg = image_b64_data.get(cam_id)
        if not raw_jpeg:
            continue
        try:
            images[cam_id] = base64.b64decode(raw_jpeg.encode("ascii"))
        except Exception:
            continue
    return images


# ─────────────────────────────────────────────
# Redis 데이터 정규화
# ─────────────────────────────────────────────
def _normalize_zone_counts(raw):
    """Redis JSON에서 zone 키를 int로 정규화."""
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"[StateManager] zone_counts JSON ERROR: {e}", file=sys.stderr, flush=True)
        return None

    if not isinstance(data, dict):
        return None

    normalized = {}
    for z_key, counts in data.items():
        try:
            z = int(z_key)
        except Exception:
            continue
        if not isinstance(counts, dict):
            continue
        normalized[z] = counts

    return normalized



# ─────────────────────────────────────────────
# 이벤트 실행 함수
# ─────────────────────────────────────────────
def execute_event(ts, zone, agg_counts, cam_images=None, debug_meta=None):
    """이벤트 후처리 (DB 저장 + 텔레그램 전송). 알람 ON은 메인 스레드에서 동기 실행."""
    try:
        helmets = agg_counts.get(zone, {}).get("person_with_helmet", 0)
        no_helmets = agg_counts.get(zone, {}).get("person_no_helmet", 0)
        total = helmets + no_helmets

        desc = f"Z{zone} 호이스트 이동 중 안전인원 부족"

        print(
            f"[StateManager EVENT] Z{zone} DANGER "
            f"(helmet:{helmets} total:{total})",
            file=sys.stderr, flush=True
        )

        if debug_meta:
            print(f"[StateManager EVENT] meta={json.dumps(debug_meta, ensure_ascii=False)}", file=sys.stderr, flush=True)

        image_paths = []
        if cam_images:
            create_directory_if_not_exists(image_save_dir)
            for cam_id, image_bytes in cam_images.items():
                if not image_bytes:
                    continue
                image_filename = os.path.join(
                    image_save_dir, f"{ts}_{cam_id}_Z{zone}_event.jpg"
                )
                with open(image_filename, "wb") as f:
                    f.write(image_bytes)
                image_paths.append((cam_id, image_filename))

        if image_paths:
            for cam_id, image_path in image_paths:
                insert_camera_event_hist(
                    ts,
                    cam_id,
                    "E001",
                    desc,
                    image_path,
                    False,
                    None,
                )
        else:
            insert_camera_event_hist(
                ts,
                "MULTI",
                "E001",
                desc,
                None,
                False,
                None,
            )

        # 텔레그램은 카메라별 이미지 전송 (없으면 MULTI 텍스트)
        if image_paths:
            for cam_id, image_path in image_paths:
                send_telegram_alert(cam_id, "Emergency", image_path)
        else:
            send_telegram_alert("MULTI", "Emergency", None)

    except Exception as e:
        print(f"[StateManager] EVENT ERROR: {e}", file=sys.stderr, flush=True)


# ─────────────────────────────────────────────
# 메인 루프
# ─────────────────────────────────────────────
def main():
    print("[StateManager] 시작", file=sys.stderr, flush=True)

    # 시작 시 알람 초기화 (이전 실행에서 알람이 켜진 채 종료된 경우 대비)
    try:
        set_alarm(0, 1)
        print("[StateManager] ALARM 초기화 (OFF)", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[StateManager] ALARM 초기화 실패: {e}", file=sys.stderr, flush=True)

    safety_sm = SafetyStateMachine()
    zone_stabilizer = ZonePeopleStabilizer()
    executor = ThreadPoolExecutor(max_workers=4)

    last_event_time = 0
    last_status_log_time = 0
    startup_time = time.time()
    consecutive_good = 0
    eval_tick = 0
    alarm_active = False

    # 카메라별 프레임 갱신 체크
    last_frame_id = {}     # cam_id -> last frame_id seen
    pending_cams = set()   # 이번 "통합 프레임"에 반영될 cam_id

    try:
        while True:
            zone_data, moving_data, frame_data, image_b64_data = read_all_camera_states()

            if not zone_data:
                consecutive_good = 0
                time.sleep(EMPTY_SLEEP_SEC)
                continue

            consecutive_good += 1

            # 1️⃣ MAX 집계 + 시간 안정화 (통합 카운트)
            #    - zone1, zone2 person을 상호 합산 (전체 인원 기준 안전 판단)
            agg_counts_raw = aggregate_max_cross_zone(
                zone_data, ALL_ZONES, TARGET_CLASSES
            )
            agg_counts = zone_stabilizer.update(agg_counts_raw)

            # 2️⃣ 프레임 갱신된 카메라 추적
            for cam_id in zone_data.keys():
                fid = frame_data.get(cam_id)
                if fid is None:
                    continue
                if last_frame_id.get(cam_id) == fid:
                    continue
                last_frame_id[cam_id] = fid
                pending_cams.add(cam_id)

            # 3️⃣ 모든 카메라가 최소 1프레임 갱신될 때만 "통합 프레임" 1회로 인정
            pending_cams &= set(zone_data.keys())  # 오프라인 카메라 제거
            if pending_cams and len(pending_cams) == len(zone_data):
                pending_cams.clear()
                eval_tick += 1
                zone_moving = {z: any(moving_data.values()) for z in ALL_ZONES}
                raw_safety = compute_safety(zone_moving, agg_counts, ALL_ZONES)
            else:
                time.sleep(LOOP_SLEEP_SEC)
                continue

            # 4️⃣ 5프레임마다 한 번 판단 (통합 프레임 기준)
            if eval_tick % EVAL_EVERY_N_FRAMES != 0:
                time.sleep(LOOP_SLEEP_SEC)
                continue

            # 5️⃣ SafetyStateMachine으로 최종 판단 (SAFE→DANGER 3초 지연)
            safety_status = safety_sm.update(raw_safety)

            # 알람 해제: DANGER 상태가 모두 해소되면 OFF
            any_danger = any(v == "DANGER" for v in safety_status.values())
            if alarm_active and not any_danger:
                try:
                    set_alarm(0, 1)
                    print("[StateManager] ALARM OFF (안전 상태 복귀)", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f"[StateManager] ALARM OFF ERROR: {e}", file=sys.stderr, flush=True)
                alarm_active = False

            # 상태 요약 로그 (주기적)
            now = time.time()
            if now - last_status_log_time >= STATUS_LOG_INTERVAL_SEC:
                cam_count = len(zone_data)
                moving_count = sum(1 for v in moving_data.values() if v)
                print(
                    f"[StateManager] cams={cam_count} moving={moving_count} " 
                    f"agg={agg_counts} safety={safety_status}",
                    file=sys.stderr, flush=True,
                )
                last_status_log_time = now

            # 5️⃣ 이벤트 실행 (DANGER) — 워밍업 중 억제
            warmup_done = (now - startup_time) >= WARMUP_SEC
            tracking_stable = consecutive_good >= MIN_STABLE_CYCLES 
            # print(
            #     f"[StateManager] eval_tick={eval_tick} warmup_done={warmup_done} "
            #     f"tracking_stable={tracking_stable} "
            #     f"since_start={now - startup_time:.1f}s "
            #     f"since_last_event={now - last_event_time:.1f}s",
            #     file=sys.stderr, flush=True,
            # )
            if not warmup_done or not tracking_stable:
                time.sleep(LOOP_SLEEP_SEC)
                continue

            for z in ALL_ZONES:
                if safety_status.get(z) != "DANGER":
                    continue

                # 이벤트 쿨다운
                if now - last_event_time < Config.EVENT_MIN_INTERVAL:
                    continue

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")

                cam_ids = list(zone_data.keys())
                cam_images = _decode_event_images(cam_ids, image_b64_data)

                debug_meta = {
                    "frame_ids": frame_data,
                    "moving_data": moving_data,
                    "zone_data": zone_data,
                    "agg_counts": agg_counts,
                    "safety_status": safety_status,
                }

                # 알람 ON을 메인 스레드에서 동기 실행 (OFF와의 순서 역전 방지)
                set_alarm(1, 1)
                print("[StateManager] ALARM ON (위험 상태 감지)", file=sys.stderr, flush=True)
                alarm_active = True

                # DB 저장 + 텔레그램 전송은 비동기
                executor.submit(execute_event, ts, z, agg_counts, cam_images, debug_meta)
                last_event_time = now

            time.sleep(LOOP_SLEEP_SEC)

    except KeyboardInterrupt:
        print("[StateManager] 종료 요청", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[StateManager] ERROR: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        executor.shutdown(wait=False)
        print("[StateManager] 종료", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
