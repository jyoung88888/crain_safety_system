import cv2
import numpy as np


def compute_iou(box1, box2):
    """두 bbox의 IOU 계산. box format: (x1, y1, x2, y2)"""
    x1_1, y1_1, x2_1, y2_1 = box1
    x1_2, y1_2, x2_2, y2_2 = box2
    xi1 = max(x1_1, x1_2)
    yi1 = max(y1_1, y1_2)
    xi2 = min(x2_1, x2_2)
    yi2 = min(y2_1, y2_2)
    inter_area = max(0, xi2 - xi1) * max(0, yi2 - yi1)
    box1_area = (x2_1 - x1_1) * (y2_1 - y1_1)
    box2_area = (x2_2 - x1_2) * (y2_2 - y1_2)
    union_area = box1_area + box2_area - inter_area
    return inter_area / union_area if union_area > 0 else 0


def get_zone_for_point(pt, roi_pts, roi_zones):
    """점 (x,y)가 ROI 다각형 안에 있으면 해당 zone, 밖이면 None"""
    for polygon, zone in zip(roi_pts, roi_zones):
        contour = polygon.reshape(-1, 1, 2)
        if cv2.pointPolygonTest(contour, (float(pt[0]), float(pt[1])), False) >= 0:
            return zone
    return None


_roi_mask_cache = {}


def _roi_signature(roi_pts):
    """ROI 변경 여부 체크용 시그니처."""
    return tuple(tuple(map(int, p.flatten().tolist())) for p in roi_pts)


def get_roi_masks(cam_id, frame_shape, roi_pts):
    """ROI 폴리곤별 마스크 리스트 생성/캐시."""
    h, w = frame_shape[:2]
    roi_sig = _roi_signature(roi_pts)
    key = (cam_id, h, w, roi_sig)
    if key in _roi_mask_cache:
        return _roi_mask_cache[key]

    masks = []
    for roi in roi_pts:
        mask = np.zeros((h, w), dtype=np.uint8)
        cv2.fillPoly(mask, [roi], 255)
        masks.append(mask)
    _roi_mask_cache[key] = masks
    return masks


def _bbox_roi_overlap_ratio(box, roi_mask):
    """bbox와 ROI 마스크의 겹침 비율(ROI∩bbox / bbox)."""
    x1, y1, x2, y2 = box
    h, w = roi_mask.shape[:2]
    x1 = max(0, min(x1, w - 1))
    x2 = max(0, min(x2, w))
    y1 = max(0, min(y1, h - 1))
    y2 = max(0, min(y2, h))
    if x2 <= x1 or y2 <= y1:
        return 0.0
    roi_slice = roi_mask[y1:y2, x1:x2]
    overlap = int(roi_slice.sum() / 255)
    area = (x2 - x1) * (y2 - y1)
    return overlap / area if area > 0 else 0.0


def get_zone_for_bbox(box, roi_pts, roi_zones, roi_masks, min_ratio):
    """bbox가 ROI와 충분히 겹치면 해당 zone 반환. 기준: overlap/bbox >= min_ratio."""
    best_zone = None
    best_ratio = 0.0
    for roi_mask, zone in zip(roi_masks, roi_zones):
        ratio = _bbox_roi_overlap_ratio(box, roi_mask)
        if ratio >= min_ratio and ratio > best_ratio:
            best_ratio = ratio
            best_zone = zone
    return best_zone


def draw_bbox(frame, box, label, color, text_color=(0, 0, 0)):
    """bbox 그리기 헬퍼."""
    x1, y1, x2, y2 = box
    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
    (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 2)
    cv2.rectangle(frame, (x1, y1 - th - 4), (x1 + tw + 2, y1), color, -1)
    cv2.putText(frame, label, (x1 + 1, y1 - 3),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, text_color, 2)
