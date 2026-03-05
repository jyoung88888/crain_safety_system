import time
from collections import deque
from config.config import Config


# ────────── MAX 집계 ──────────
def aggregate_max(cam_data, zones, keys):
    """각 카메라의 카운트를 MAX로 집계 (중복 카운팅 방지)."""
    agg = {z: {k: 0 for k in keys} for z in zones}
    for z in zones:
        for k in keys:
            values = [cam_data[cid][z][k] for cid in cam_data if z in cam_data[cid]]
            if values:
                agg[z][k] = max(values)
    return agg


def aggregate_max_cross_zone(cam_data, zones, keys):
    """기본 MAX 집계 후, zone1과 zone2의 person을 상호 합산.

    1층 1명 + 2층 1명 = 양쪽 모두 2명으로 판단.
    호이스트가 1~2층을 오가므로 전체 인원으로 안전 판단해야 함.
    """
    agg = aggregate_max(cam_data, zones, keys)

    if 1 not in zones or 2 not in zones:
        return agg

    z1 = agg.get(1, {})
    z2 = agg.get(2, {})

    person_keys = ["person_with_helmet", "person_no_helmet"]
    total = {}
    for k in person_keys:
        total[k] = z1.get(k, 0) + z2.get(k, 0)

    for z in [1, 2]:
        if z not in agg:
            agg[z] = {k: 0 for k in keys}
        for k in person_keys:
            agg[z][k] = total[k]

    return agg




class ZonePeopleStabilizer:
    """인원 카운트 시간 안정화 — 최근 N초 중 max값 유지."""

    def __init__(self):
        self._history = {}  # zone → deque of (timestamp, {class: count})

    def update(self, agg_counts, now=None):
        if now is None:
            now = time.time()
        window = Config.ZONE_COUNT_WINDOW
        result = {}
        for z, counts in agg_counts.items():
            if z not in self._history:
                self._history[z] = deque()
            self._history[z].append((now, dict(counts)))
            # 윈도우 밖 오래된 데이터 제거
            while self._history[z] and now - self._history[z][0][0] > window:
                self._history[z].popleft()
            # 윈도우 내 각 클래스별 max
            stabilized = {}
            for k in counts:
                stabilized[k] = max(snap[k] for _, snap in self._history[z] if k in snap)
            result[z] = stabilized
        return result


# ────────── 안전 판단 ──────────
def compute_safety(zone_moving, agg_counts, all_zones):
    """호이스트 작동 + 인원 수 기반 안전 판단."""
    safety_status = {}
    for z in all_zones:
        if zone_moving.get(z, False):
            helmets = agg_counts.get(z, {}).get("person_with_helmet", 0)
            no_helmets = agg_counts.get(z, {}).get("person_no_helmet", 0)
            total = helmets + no_helmets
            if total == 0:
                safety_status[z] = None  # 원격 조종
            elif total >= Config.SAFETY_MIN_PEOPLE and helmets >= Config.SAFETY_MIN_HELMETS:
                safety_status[z] = "SAFE"
            else:
                safety_status[z] = "DANGER"
        else:
            safety_status[z] = None
    return safety_status


class SafetyStateMachine:
    """안전 상태 히스테리시스 — SAFE→DANGER 전환만 지연."""

    def __init__(self):
        self._state = {}           # zone → "SAFE" / "DANGER" / None
        self._danger_since = {}    # zone → DANGER 조건 최초 감지 시각

    def update(self, raw_safety, now=None):
        if now is None:
            now = time.time()
        result = {}
        for z, raw in raw_safety.items():
            prev = self._state.get(z)
            if raw == "DANGER":
                if prev == "DANGER":
                    result[z] = "DANGER"
                else:
                    if self._danger_since.get(z) is None:
                        self._danger_since[z] = now
                    if now - self._danger_since[z] >= Config.SAFETY_DANGER_DURATION:
                        result[z] = "DANGER"
                        self._danger_since[z] = None
                    else:
                        result[z] = prev if prev else "SAFE"
            else:
                result[z] = raw  # SAFE / None 즉시 반영
                self._danger_since[z] = None
            self._state[z] = result[z]
        return result
