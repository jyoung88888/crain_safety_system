def calculate_overlap_area(box1, box2):
    """
    두 바운딩 박스의 겹치는 영역의 넓이를 계산합니다.
    
    Args:
        box1: 첫 번째 박스 [x1, y1, x2, y2] 형식
        box2: 두 번째 박스 [x1, y1, x2, y2] 형식
        
    Returns:
        float: 겹치는 영역의 넓이
    """
    # 박스 좌표 추출
    box1_x1, box1_y1, box1_x2, box1_y2 = box1
    box2_x1, box2_y1, box2_x2, box2_y2 = box2
    
    # 겹치는 영역의 좌표 계산
    x_left = max(box1_x1, box2_x1)
    y_top = max(box1_y1, box2_y1)
    x_right = min(box1_x2, box2_x2)
    y_bottom = min(box1_y2, box2_y2)
    
    # 겹치는 영역이 없는 경우
    if x_right < x_left or y_bottom < y_top:
        return 0.0
    
    # 겹치는 영역의 넓이 계산
    intersection_area = (x_right - x_left) * (y_bottom - y_top)
    return intersection_area

def calculate_box_area(box):
    """
    바운딩 박스의 넓이를 계산합니다.
    
    Args:
        box: 박스 [x1, y1, x2, y2] 형식
        
    Returns:
        float: 박스의 넓이
    """
    x1, y1, x2, y2 = box
    return (x2 - x1) * (y2 - y1)

def is_box_contained(inner_box, outer_box, threshold=0.8):
    """
    한 바운딩 박스가 다른 바운딩 박스 안에 지정된 비율 이상 포함되어 있는지 확인합니다.
    
    Args:
        inner_box: 내부 박스 [x1, y1, x2, y2] 형식
        outer_box: 외부 박스 [x1, y1, x2, y2] 형식
        threshold: 포함 비율 임계값 (기본값: 0.8, 즉 80%)
        
    Returns:
        bool: 내부 박스가 외부 박스에 지정된 비율 이상 포함되면 True, 아니면 False
    """
    # 내부 박스의 넓이 계산
    inner_area = calculate_box_area(inner_box)
    
    # 겹치는 영역의 넓이 계산
    overlap_area = calculate_overlap_area(inner_box, outer_box)
    
    # 겹치는 비율 계산
    if inner_area == 0:  # 0으로 나누기 방지
        return False
    
    overlap_ratio = overlap_area / inner_area
    
    # 디버깅용 출력 (로그 출력 생략)
    # print(f"내부 박스 넓이: {inner_area}, 겹치는 영역 넓이: {overlap_area}, 겹치는 비율: {overlap_ratio:.2f}")
    
    # 지정된 임계값 이상 겹치면 True 반환
    return overlap_ratio >= threshold