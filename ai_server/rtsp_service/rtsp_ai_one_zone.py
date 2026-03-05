import os
import sys

# subprocess로 실행 시 상위 디렉토리(/app)를 모듈 검색 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import cv2
import numpy as np
import multiprocessing as mp
import time
import threading
import torch
import gc
from ultralytics import YOLO
import argparse
import redis
import json
import base64
from lib.public_func import server, reconnect_video_capture, \
    get_roi, current_directory
from lib.hoist_tracker import HoistTracker, HoistIDAssigner
from lib.detection_utils import (
    compute_iou, get_zone_for_point, get_roi_masks,
    get_zone_for_bbox, draw_bbox,
)
from lib.streaming import FrameCache, Grabber, rtmp_stream_func
from config.config import Config


# ═══════════════════════════════════════════════════════════════
#  1. Redis 함수
# ═══════════════════════════════════════════════════════════════
redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    decode_responses=True
)


def redis_write_camera_snapshot(cctv_id, zone_counts, hoist_moving, frame_id, jpeg_b64=None):
    """카메라별 상태를 저장하고, jpeg_b64가 있으면 이미지도 함께 갱신."""
    key_snapshot = f"cam:{cctv_id}:snapshot"
    ttl_sec = min(5, int(Config.LATEST_JPEG_TTL_SEC))
    try:
        mapping = {
            "zone_counts_json": json.dumps(zone_counts),
            "hoist_moving": "1" if hoist_moving else "0",
            "frame_id": str(frame_id),
            "captured_ts": str(time.time()),
        }
        if jpeg_b64 is not None:
            mapping["jpeg_b64"] = jpeg_b64

        pipe = redis_client.pipeline()
        pipe.hset(
            key_snapshot,
            mapping=mapping,
        )
        pipe.expire(key_snapshot, ttl_sec)
        pipe.execute()
    except Exception as e:
        print(f"[Redis SNAPSHOT WRITE ERROR] {e}", file=sys.stderr, flush=True)


# ═══════════════════════════════════════════════════════════════
#  2. AI 영상 처리 (핵심 로직 — 멀티카메라 배치)
# ═══════════════════════════════════════════════════════════════
TARGET_CLASSES = ["person_with_helmet", "person_no_helmet", "hoist"]


# ────────── 카메라별 프레임 처리 (detection_view.py 동일) ──────────
def draw_frame(frame, result, roi_pts, roi_zones, cam_id, hoist_tracker, hoist_id_assigner, fixed_zone=None):
    """원본 프레임 위에 감지 결과 + Person-Helmet IOU matching 그리기
    return: zone_counts, hoist_draw_info
    """
    zones_present = sorted(set(roi_zones))
    zone_counts = {z: {cls: 0 for cls in TARGET_CLASSES} for z in zones_present}

    # ── 1단계: person, helmet, hoist 분리 수집 ──
    person_boxes = []
    helmet_boxes = []
    hoist_centers_tmp = []
    hoist_data_tmp = []

    roi_masks = None
    min_ratio = Config.PERSON_ROI_OVERLAP_MIN
    if roi_pts:
        roi_masks = get_roi_masks(cam_id, frame.shape, roi_pts)

    if result.boxes is not None:
        for b in result.boxes:
            x1, y1, x2, y2 = map(int, b.xyxy[0])
            name = result.names[int(b.cls[0])]
            conf = float(b.conf[0])
            center = ((x1 + x2) // 2, (y1 + y2) // 2)
            zone = get_zone_for_point(center, roi_pts, roi_zones) if roi_pts else None

            if name == "person":
                # ROI 포함 기준: person bbox와 ROI의 overlap/bbox >= min_ratio
                p_zone = get_zone_for_bbox((x1, y1, x2, y2), roi_pts, roi_zones, roi_masks, min_ratio) if roi_masks else None
                if p_zone is not None:
                    person_boxes.append((x1, y1, x2, y2, conf, center, p_zone))
            elif name == "helmet":
                helmet_boxes.append((x1, y1, x2, y2, conf))
            elif name == "hoist":
                hoist_zone = fixed_zone if zone is not None else None
                hoist_centers_tmp.append(center)
                hoist_data_tmp.append((x1, y1, x2, y2, conf, center, hoist_zone))

    # 카메라별 자체 ID 배정
    hoist_boxes = []
    if hoist_centers_tmp:
        assigned_ids = hoist_id_assigner.assign(cam_id, hoist_centers_tmp)
        for data, track_id in zip(hoist_data_tmp, assigned_ids):
            hoist_boxes.append((*data, track_id))

    # ── 2단계: Person-Helmet IOU 매칭 ──
    matched_helmet_indices = set()
    person_with_helmet = []
    person_no_helmet = []

    for pbox in person_boxes:
        px1, py1, px2, py2, p_conf, p_center, p_zone = pbox
        best_iou = 0
        best_idx = -1
        for h_idx, hbox in enumerate(helmet_boxes):
            if h_idx in matched_helmet_indices:
                continue
            iou = compute_iou((px1, py1, px2, py2), (hbox[0], hbox[1], hbox[2], hbox[3]))
            if iou > best_iou:
                best_iou = iou
                best_idx = h_idx
        if best_iou >= Config.IOU_THRESHOLD:
            matched_helmet_indices.add(best_idx)
            person_with_helmet.append(pbox)
            zone_counts[p_zone]["person_with_helmet"] += 1
        else:
            person_no_helmet.append(pbox)
            zone_counts[p_zone]["person_no_helmet"] += 1

    # ── 3단계: Person 그리기 ──
    for px1, py1, px2, py2, p_conf, _, p_zone in person_with_helmet:
        draw_bbox(frame, (px1, py1, px2, py2), f"person+helmet {p_conf:.2f}", (0, 255, 0))
    for px1, py1, px2, py2, p_conf, _, p_zone in person_no_helmet:
        draw_bbox(frame, (px1, py1, px2, py2), f"NO-HELMET {p_conf:.2f}", (0, 0, 255), (255, 255, 255))

    # ── 4단계: Hoist 이동 추적 (bbox 그리기는 zone_moving 확정 후 메인 루프에서) ──
    primary_hoist_idx = -1
    if fixed_zone is not None and len(hoist_boxes) > 1:
        max_area = 0
        for hi, hd in enumerate(hoist_boxes):
            area = (hd[2] - hd[0]) * (hd[3] - hd[1])
            if area > max_area:
                max_area = area
                primary_hoist_idx = hi

    hoist_draw_info = []
    for hi, hdata in enumerate(hoist_boxes):
        hx1, hy1, hx2, hy2, h_conf, h_center, h_zone, track_id = hdata
        if primary_hoist_idx != -1 and hi != primary_hoist_idx:
            continue
        bbox_size = max(hx2 - hx1, hy2 - hy1)
        hoist_tracker.update(cam_id, track_id, h_center, h_zone, bbox_size)
        if h_zone is not None:
            zone_counts[h_zone]["hoist"] += 1
        hoist_draw_info.append((hx1, hy1, hx2, hy2, h_conf, track_id, h_zone))

    # ── ROI 반투명 영역 ──
    if roi_pts:
        overlay = frame.copy()
        for roi, zone in zip(roi_pts, roi_zones):
            cv2.fillPoly(overlay, [roi], Config.ZONE_COLORS.get(zone, Config.DEFAULT_ZONE_COLOR))
        frame[:] = cv2.addWeighted(frame, 0.85, overlay, 0.15, 0)

        for roi, zone in zip(roi_pts, roi_zones):
            color = Config.ZONE_COLORS.get(zone, Config.DEFAULT_ZONE_COLOR)
            cv2.polylines(frame, [roi], True, color, 2)

    return zone_counts, hoist_draw_info


# ────────── AI 보조 함수들 ──────────
def _init_yolo_model():
    """GPU / YOLO 모델 초기화 및 디바이스 반환"""
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        gc.collect()
        # PyTorch VRAM 점유 제한 (NVDEC 3스트림 + NVENC 3출력 여유 확보)
        torch.cuda.set_per_process_memory_fraction(0.5)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"[AI PROCESS] Device: {device}, CUDA: {torch.cuda.is_available()}", flush=True)

    yolo_model_detection = YOLO(current_directory + '/model/run0119.pt', verbose=False).to(device)
    return yolo_model_detection, device


def _start_grabbers(cameras, cache, stop_event):
    """카메라별 Grabber 스레드 시작"""
    grabbers = []
    for cam in cameras:
        g = Grabber(
            cam['cctv_id'],
            cam['in_url'],
            cache,
            stop_event,
            cam['width'],
            cam['height'],
        )
        g.start()
        grabbers.append(g)
        print(f"[{cam['cctv_id']}] Grabber 시작: {cam['in_url']} ({cam['width']}x{cam['height']})", flush=True)
    return grabbers


def _load_roi_config(cameras):
    """DB에서 ROI/Zone 정보를 로드 후 맵과 cam_config 구성"""
    roi_map = {}
    zone_map = {}
    cam_config = {}
    for cam in cameras:
        cctv_id = cam['cctv_id']
        try:
            roi_arr, roi_zones = get_roi(cctv_id)
            if roi_arr and roi_zones:
                roi_pts = [np.array(poly, dtype=np.int32) for poly in roi_arr]
                fixed_zone = roi_zones[0] if roi_zones else 1
            else:
                roi_pts = []
                roi_zones = []
                fixed_zone = 1
        except Exception as e:
            print(f"[{cctv_id}] ROI load error: {e}", file=sys.stderr, flush=True)
            roi_pts = []
            roi_zones = []
            fixed_zone = 1

        roi_map[cctv_id] = roi_pts
        zone_map[cctv_id] = roi_zones
        cam['fixed_zone'] = fixed_zone
        cam['roi_zones'] = roi_zones
        cam_config[cctv_id] = cam
        print(f"[{cctv_id}] ROI: {len(roi_pts)} polygons, zones={roi_zones}, fixed_zone={fixed_zone}", flush=True)

    all_zones = sorted(set(z for cam in cameras for z in cam.get('roi_zones', [])))
    if not all_zones:
        all_zones = [1]

    return roi_map, zone_map, cam_config, all_zones


def _wait_first_frames(cache, cameras, stop_event, timeout=20):
    """최소 1대 이상 프레임 도착 시 시작, 타임아웃 시에도 진행"""
    print("[AI] 첫 프레임 대기 중...", flush=True)
    deadline = time.time() + timeout
    while not stop_event.is_set() and time.time() < deadline:
        ready = [cam['cctv_id'] for cam in cameras if cache.get(cam['cctv_id']) is not None]
        if len(ready) >= 1:
            if len(ready) < len(cameras):
                missing = [c['cctv_id'] for c in cameras if c['cctv_id'] not in ready]
                print(f"[AI] {len(ready)}대 연결 완료, 미연결: {missing} → 부분 시작", flush=True)
            else:
                print("[AI] 모든 카메라 연결 완료", flush=True)
            return
        time.sleep(0.1)
    ready = [cam['cctv_id'] for cam in cameras if cache.get(cam['cctv_id']) is not None]
    if ready:
        print(f"[AI] 타임아웃 ({timeout}s) — {len(ready)}대로 시작", flush=True)
    else:
        print(f"[AI] 타임아웃 ({timeout}s) — 연결된 카메라 없음, 대기 계속", flush=True)


def _collect_latest_frames(cache, cameras, max_age=5.0):
    """각 카메라별 최신 프레임 수집 (max_age초 이상 오래된 프레임은 스킵)"""
    frames = []
    cam_ids = []
    for cam in cameras:
        cid = cam['cctv_id']
        f = cache.get(cid)
        if f is None:
            continue
        age = cache.get_age(cid)
        if age > max_age:
            print(f"[{cid}] 프레임 만료 ({age:.1f}s > {max_age}s) → 스킵", flush=True)
            continue
        frames.append(f)
        cam_ids.append(cid)
    return frames, cam_ids


def _run_inference(yolo_model, frames, device):
    """YOLO 배치 추론 래퍼"""
    return yolo_model.predict(frames, conf=Config.CONF_THRESHOLD, verbose=False, device=device)


def _process_results(
    frames,
    cam_ids,
    results,
    roi_map,
    zone_map,
    cam_config,
    hoist_tracker,
    hoist_id_assigner,
):
    """YOLO 결과를 바탕으로 시각화/카운팅/호이스트 정보 계산"""

    drawn = []
    cam_zone_counts = {}
    cam_hoist_info = {}

    for i, cam_id in enumerate(cam_ids):
        vis = frames[i].copy()
        fixed_zone = cam_config[cam_id].get('fixed_zone')
        zcounts, hoist_info = draw_frame(
            vis,
            results[i],
            roi_map[cam_id],
            zone_map[cam_id],
            cam_id,
            hoist_tracker,
            hoist_id_assigner,
            fixed_zone,
        )
        drawn.append(vis)
        cam_zone_counts[cam_id] = zcounts
        cam_hoist_info[cam_id] = hoist_info

    # ── 호이스트 bbox 그리기 ──
    for i, cam_id in enumerate(cam_ids):
        for hx1, hy1, hx2, hy2, h_conf, track_id, _ in cam_hoist_info[cam_id]:
            if hoist_tracker.is_moving(cam_id, track_id):
                bbox_color = (0, 255, 255)
                label = f"hoist ID{track_id} {h_conf:.2f} WORKING"
            else:
                bbox_color = (0, 255, 0)
                label = f"hoist ID{track_id} {h_conf:.2f}"
            draw_bbox(drawn[i], (hx1, hy1, hx2, hy2), label, bbox_color)

    return drawn, cam_zone_counts, cam_hoist_info


def _push_frames_to_queues(drawn, cam_ids, result_queues, image_queues):
    """RTMP/웹 전송용 큐에 프레임 푸시"""
    for i, cam_id in enumerate(cam_ids):
        rq = result_queues.get(cam_id)
        if rq and not rq.full():
            rq.put(drawn[i])

        iq = image_queues.get(cam_id)
        if not iq:
            continue

        try:
            iq.get_nowait()
        except Exception:
            pass
        iq.put(drawn[i])


# ────────── AI 영상 처리 메인 함수 (멀티카메라 배치) ──────────
def ai_process_func(counter, result_queues, image_queues, cameras):
    """
    Camera Process 전용 AI 처리 프로세스

    역할:
    - RTSP 입력
    - YOLO 추론
    - draw_frame
    - hoist_moving / zone_counts 계산
    - Redis에 카메라별 상태 저장
    - RTMP / 웹 프레임 전송
    """

    # YOLO 초기화
    yolo_model_detection, device = _init_yolo_model()

    stop = threading.Event()
    cache = FrameCache()

    # Grabber 시작
    grabbers = _start_grabbers(cameras, cache, stop)

    # ROI 로드
    roi_map, zone_map, cam_config, _all_zones = _load_roi_config(cameras)

    hoist_tracker = HoistTracker()
    hoist_id_assigner = HoistIDAssigner(max_dist=Config.HOIST_MAX_JUMP)
    frame_count = 0
    frame_seq = {}
    last_jpeg_time = {}
    target_fps = cameras[0].get('fps', 25)
    frame_interval = 1.0 / max(1, target_fps)
    fps_tick = 0
    last_fps_ts = time.time()

    # 첫 프레임 대기
    _wait_first_frames(cache, cameras, stop)

    try:
        while not stop.is_set():
            loop_start = time.time()

            frames, cam_ids = _collect_latest_frames(cache, cameras)

            if len(frames) == 0:
                time.sleep(0.05)
                continue

            # YOLO 배치 추론
            results = _run_inference(yolo_model_detection, frames, device)

            # 카메라별 계산
            (
                drawn,
                cam_zone_counts,
                cam_hoist_info,
            ) = _process_results(
                frames,
                cam_ids,
                results,
                roi_map,
                zone_map,
                cam_config,
                hoist_tracker,
                hoist_id_assigner,
            )

            cam_frames = {cid: drawn[i] for i, cid in enumerate(cam_ids)}

            # Redis에 카메라별 상태 저장
            try:
                for cam_id, zcounts in cam_zone_counts.items():
                    frame = cam_frames.get(cam_id)
                    if frame is None:
                        continue
                    hoist_moving = hoist_tracker.is_any_hoist_moving(cam_id)
                    frame_seq[cam_id] = frame_seq.get(cam_id, 0) + 1
                    now = time.time()
                    jpeg_b64 = None
                    if now - last_jpeg_time.get(cam_id, 0) >= Config.LATEST_JPEG_INTERVAL_SEC:
                        ok, jpeg_buf = cv2.imencode(
                            '.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, Config.LATEST_JPEG_QUALITY]
                        )
                        if ok:
                            jpeg_b64 = base64.b64encode(jpeg_buf.tobytes()).decode("ascii")
                            last_jpeg_time[cam_id] = now
                    redis_write_camera_snapshot(
                        cam_id,
                        zcounts,
                        hoist_moving,
                        frame_seq[cam_id],
                        jpeg_b64,
                    )
            except Exception as e:
                print(f"[Redis WRITE ERROR] {e}", file=sys.stderr, flush=True)

            # AI 처리 완료 → watchdog heartbeat
            with counter.get_lock():
                counter.value += 1

            # RTMP / 웹 전송
            _push_frames_to_queues(drawn, cam_ids, result_queues, image_queues)

            # 주기적 정리
            frame_count += 1
            fps_tick += 1
            now_ts = time.time()
            if now_ts - last_fps_ts >= 1.0:
                fps = fps_tick / (now_ts - last_fps_ts)
                print(f"[AI] FPS={fps:.1f} (frames={fps_tick})", flush=True)
                fps_tick = 0
                last_fps_ts = now_ts
            if frame_count % 100 == 0:
                hoist_tracker.cleanup(max_age=5.0)

            # FPS 제한
            elapsed = time.time() - loop_start
            if elapsed < frame_interval:
                time.sleep(frame_interval - elapsed)

    except KeyboardInterrupt:
        print("[AI] 중단", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"[AI] Error: {e}", file=sys.stderr, flush=True)
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        stop.set()
        for g in grabbers:
            g.join(timeout=3)
        print("[AI] 종료", flush=True)


# ═══════════════════════════════════════════════════════════════
#  3. 프로세스 관리 (워치독 / 정리 / 메인)
# ═══════════════════════════════════════════════════════════════

# ────────── 프로세스 및 큐 정리 ──────────
def cleanup_processes(processes, queues):
    """프로세스와 큐를 정리하는 함수."""
    for process in processes:
        if process.is_alive():
            print(f"Terminating process {process.name}...")
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                print(f"Failed to terminate process {process.name}.")

    for q in queues:
        try:
            while not q.empty():
                q.get_nowait()
        except Exception:
            pass
        q.close()
        q.join_thread()

    print("Processes and queues have been successfully cleaned up.")


# ────────── 워치독 (멀티카메라 통합 모니터링) ──────────
def watchdog(counter, cameras, timeout=30):
    """모든 카메라의 AI 처리 + RTMP 스트리밍 + ZMQ 서버를 통합 모니터링."""
    set_fps = cameras[0].get('fps', 25)
    stall_streak = 0          # 연속 정체 횟수
    MAX_STALL_BEFORE_RESTART = 2  # 2회 연속 정체 시 재시작 (= 60초 유예)

    def _create_and_start():
        """큐 + 프로세스 생성 및 시작"""
        result_queues = {}
        image_queues = {}
        grid_st_queues = {}

        for cam in cameras:
            cid = cam['cctv_id']
            result_queues[cid] = mp.Queue(maxsize=10)
            image_queues[cid] = mp.Queue(maxsize=1)
            grid_st_queues[cid] = mp.Queue(maxsize=1)

        ai_process = mp.Process(
            target=ai_process_func,
            args=(counter, result_queues, image_queues, cameras))

        stream_processes = {}
        server_processes = {}

        for cam in cameras:
            cid = cam['cctv_id']
            stream_processes[cid] = mp.Process(
                target=rtmp_stream_func,
                args=(result_queues[cid], cam['rtmp_url'], cam['width'], cam['height'], set_fps))
            server_processes[cid] = mp.Process(
                target=server,
                args=(image_queues[cid], grid_st_queues[cid], cam['port']))

        for cid in server_processes:
            server_processes[cid].start()
        time.sleep(2)
        ai_process.start()
        for cid in stream_processes:
            stream_processes[cid].start()

        all_processes = [ai_process] + list(stream_processes.values()) + list(server_processes.values())
        all_queues = list(result_queues.values()) + list(image_queues.values()) + list(grid_st_queues.values())

        return all_processes, all_queues

    all_processes, all_queues = _create_and_start()
    last_count = counter.value

    try:
        while True:
            time.sleep(timeout)
            with counter.get_lock():
                current_count = counter.value

            if current_count == last_count:
                stall_streak += 1
                print(f"[Watchdog] 정체 감지 ({stall_streak}/{MAX_STALL_BEFORE_RESTART})", file=sys.stderr, flush=True)

                if stall_streak >= MAX_STALL_BEFORE_RESTART:
                    print("[Watchdog] 프로세스 재시작 중...", file=sys.stderr, flush=True)
                    cleanup_processes(all_processes, all_queues)
                    time.sleep(5)
                    counter.value = 0
                    stall_streak = 0

                    all_processes, all_queues = _create_and_start()
                    print("[Watchdog] 재시작 완료", flush=True)
            else:
                stall_streak = 0

            last_count = current_count
    except KeyboardInterrupt:
        print("[Watchdog] 종료 중...", flush=True)
    finally:
        cleanup_processes(all_processes, all_queues)


# ────────── 메인 함수 (멀티카메라 시스템 초기화) ──────────
def main(cameras, rtmp_host=None, rtmp_port=None):
    """RTSP → 배치 AI 처리 → RTMP 스트리밍 시스템을 초기화하고 시작."""
    if not rtmp_host:
        rtmp_host = os.getenv("RTMP_HOST", "192.168.0.13")
    if not rtmp_port:
        rtmp_port = int(os.getenv("RTMP_PORT", "1935"))
    rtmp_base = f"rtmp://{rtmp_host}:{rtmp_port}/"

    for cam in cameras:
        cam['rtmp_url'] = rtmp_base + cam['out_path']
        print(f"[{cam['cctv_id']}] RTMP: {cam['rtmp_url']}", flush=True)

        cap = reconnect_video_capture(cam['in_url'])
        if cap is None:
            print(f"[{cam['cctv_id']}] Failed to connect to RTSP stream.", file=sys.stderr, flush=True)
            return

        cam['width'] = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        cam['height'] = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        cam['fps'] = fps if fps > 0 else 25
        cap.release()

        print(f"[{cam['cctv_id']}] {cam['width']}x{cam['height']} @ {cam['fps']}fps", flush=True)

    counter = mp.Value('i', 0)
    watchdog_process = mp.Process(target=watchdog, args=(counter, cameras))
    watchdog_process.start()

    try:
        while watchdog_process.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        if watchdog_process.is_alive():
            watchdog_process.terminate()
            watchdog_process.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="멀티카메라 AI 안전관리 시스템")

    parser.add_argument('--cctv_ids', type=str, required=True,
                        help="카메라 ID (쉼표 구분, 예: cam1,cam2,cam3)")
    parser.add_argument('--in_urls', type=str, required=True,
                        help="RTSP URL (쉼표 구분)")
    parser.add_argument('--out_paths', type=str, required=True,
                        help="RTMP 출력 경로 (쉼표 구분)")
    parser.add_argument('--ports', type=str, required=True,
                        help="ZMQ 포트 (쉼표 구분)")
    parser.add_argument('--rtmp_host', type=str, default=None, help="RTMP host")
    parser.add_argument('--rtmp_port', type=int, default=None, help="RTMP port")

    args = parser.parse_args()

    cctv_ids = args.cctv_ids.split(',')
    in_urls = args.in_urls.split(',')
    out_paths = args.out_paths.split(',')
    ports = args.ports.split(',')

    cameras = []
    for cid, url, path, port in zip(cctv_ids, in_urls, out_paths, ports):
        cameras.append({
            'cctv_id': cid.strip(),
            'in_url': url.strip(),
            'out_path': path.strip(),
            'port': port.strip(),
        })

    main(cameras, args.rtmp_host, args.rtmp_port)