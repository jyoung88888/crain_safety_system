import time
from collections import deque
from config.config import Config


class HoistTracker:
    """카메라별 자체 ID 기반 호이스트 추적. key = (cam_id, track_id)

    상태 머신:
        IDLE → WORKING : 누적거리 >= threshold 가 WORKING_DURATION 이상 지속
        WORKING → IDLE : 누적거리 < threshold 가 IDLE_DURATION 이상 지속
    """
    def __init__(self):
        self._centers     = {}
        self._state       = {}
        self._condition_start = {}
        self._last_seen   = {}
        self._zones       = {}

    def _key(self, cam_id, track_id):
        return (cam_id, int(track_id))

    def _get_accumulated_dist(self, k):
        pts = self._centers.get(k)
        if not pts or len(pts) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(pts)):
            dx = pts[i][0] - pts[i-1][0]
            dy = pts[i][1] - pts[i-1][1]
            d = (dx*dx + dy*dy) ** 0.5
            if d > Config.HOIST_NOISE_GATE:
                total += d
        return total

    def update(self, cam_id, track_id, center, zone, bbox_size=100):
        k = self._key(cam_id, track_id)
        if k not in self._centers:
            self._centers[k] = deque(maxlen=Config.HOIST_WINDOW)
            self._state[k] = "IDLE"
            self._condition_start[k] = None

        if len(self._centers[k]) > 0:
            prev = self._centers[k][-1]
            dx = center[0] - prev[0]
            dy = center[1] - prev[1]
            dist = (dx*dx + dy*dy) ** 0.5
            if dist > Config.HOIST_MAX_JUMP:
                self._centers[k].clear()
                self._state[k] = "IDLE"
                self._condition_start[k] = None

        self._centers[k].append(center)
        self._last_seen[k] = time.time()
        self._zones[k] = zone

        moving_threshold = bbox_size * Config.HOIST_MOVING_RATIO
        stop_threshold   = bbox_size * Config.HOIST_STOP_RATIO

        acc_dist = self._get_accumulated_dist(k)
        state = self._state.get(k, "IDLE")
        now = time.time()

        if state == "IDLE":
            moving = acc_dist >= moving_threshold
            if moving:
                if self._condition_start[k] is None:
                    self._condition_start[k] = now
                elapsed = now - self._condition_start[k]
                if elapsed >= Config.HOIST_WORKING_DURATION:
                    self._state[k] = "WORKING"
                    self._condition_start[k] = None
            else:
                self._condition_start[k] = None
        else:  # WORKING
            if acc_dist < stop_threshold:
                if self._condition_start[k] is None:
                    self._condition_start[k] = now
                elapsed = now - self._condition_start[k]
                if elapsed >= Config.HOIST_IDLE_DURATION:
                    self._state[k] = "IDLE"
                    self._condition_start[k] = None
            else:
                self._condition_start[k] = None

    def is_moving(self, cam_id, track_id):
        k = self._key(cam_id, track_id)
        return self._state.get(k, "IDLE") == "WORKING"

    def cleanup(self, max_age=5.0):
        now = time.time()
        to_remove = [k for k, last_t in self._last_seen.items() if now - last_t > max_age]
        for k in to_remove:
            for d in (self._centers, self._state,
                      self._condition_start, self._last_seen, self._zones):
                d.pop(k, None)

    def is_any_hoist_moving(self, cam_id):
        for (c, tid) in self._centers.keys():
            if c == cam_id and self._state.get((c, tid), "IDLE") == "WORKING":
                return True
        return False


class HoistIDAssigner:
    """ByteTrack 대신 카메라별 독립 ID 배정."""
    def __init__(self, max_dist=150):
        self._tracks = {}
        self._next_id = {}
        self._max_dist = max_dist

    def assign(self, cam_id, centers):
        if cam_id not in self._tracks:
            self._tracks[cam_id] = {}
            self._next_id[cam_id] = 1

        prev = self._tracks[cam_id]
        assigned_ids = []
        new_tracks = {}
        used_prev = set()

        for center in centers:
            best_id = None
            best_dist = self._max_dist
            for tid, prev_center in prev.items():
                if tid in used_prev:
                    continue
                dx = center[0] - prev_center[0]
                dy = center[1] - prev_center[1]
                dist = (dx*dx + dy*dy) ** 0.5
                if dist < best_dist:
                    best_dist = dist
                    best_id = tid

            if best_id is not None:
                used_prev.add(best_id)
                assigned_ids.append(best_id)
                new_tracks[best_id] = center
            else:
                new_id = self._next_id[cam_id]
                self._next_id[cam_id] += 1
                assigned_ids.append(new_id)
                new_tracks[new_id] = center

        self._tracks[cam_id] = new_tracks
        return assigned_ids
