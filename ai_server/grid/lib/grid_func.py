import cv2
import numpy as np
import math
import os
import sys
import json
import base64
from datetime import datetime, timedelta
from lib.db import get_connection
import psycopg2

# 현재 파일이 있는 디렉토리 경로
current_dir = os.path.dirname(os.path.abspath(__file__))

# 상위 디렉토리 경로
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))

# 상위 디렉토리를 sys.path에 추가
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# 이제 상위 폴더에 있는 라이브러리를 임포트할 수 있습니다.

# load_image(image_path) : 이미지 불러오기
def load_image(image_path):
    """
    이미지를 불러오는 함수.

    Parameters:
        image_path (str): 이미지 경로 (grid/ 디렉토리 기준)

    Returns:
        numpy.ndarray: 불러온 이미지 배열
    """
    # 현재 스크립트 파일의 디렉토리 경로
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # grid/ 디렉토리 경로
    full_path = os.path.join(base_dir, image_path)  # grid/ 경로와 image_path 결합

    # 경로 디버깅
    print(f"Full image path: {full_path}")

    # 이미지 존재 여부 확인
    if not os.path.isfile(full_path):
        raise ValueError(f"이미지 파일이 존재하지 않습니다: {full_path}")

    # 이미지 로드
    image = cv2.imread(full_path)
    if image is None:
        raise ValueError(f"이미지를 로드할 수 없습니다: {full_path}")
    return image


# decode_image_base64(base64_str) : Base64 문자열을 OpenCV 이미지로 변환
def decode_image_base64(image_base64):
    """
    Decode a Base64 image string to an OpenCV image.
    """
    try:
        image_data = base64.b64decode(image_base64)
        np_arr = np.frombuffer(image_data, np.uint8)
        image = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        return image
    except Exception as e:
        raise ValueError(f"Failed to decode image_base64: {str(e)}")


# decode_base64_to_image(base64_str) : Base64 문자열 디코딩
def decode_base64_to_image(base64_string):
    try:
        image_data = base64.b64decode(base64_string)
        np_array = np.frombuffer(image_data, dtype=np.uint8)
        return cv2.imdecode(np_array, cv2.IMREAD_COLOR)
    except Exception as e:
        raise ValueError(f"Failed to decode Base64 string to image: {e}")


# 2.거리 및 확장 관련 계산

# calculate_distance(x1, y1, x2, y2) : 두 점 사이의 거리 계산
def calculate_distance(x1, y1, x2, y2):
    """
    두 점 (x1, y1)와 (x2, y2) 사이의 거리를 계산하는 함수
    """
    distance = math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)
    return int(distance)


# find_extended_point(x1, y1, x2, y2, width) : 방향 벡터를 기반으로 확장된 점 좌표 계산
def find_extended_point(x1, y1, x2, y2, width):
    # 두 점 사이의 방향 벡터 계산
    dx = x2 - x1
    dy = y2 - y1

    # 벡터의 길이 계산
    length = math.sqrt(dx ** 2 + dy ** 2)
    if length == 0:
        raise ValueError("두 점이 동일합니다. 벡터 방향을 계산할 수 없습니다.")

    # 단위 방향 벡터 계산
    dx_unit = dx / length
    dy_unit = dy / length

    # 폭 만큼 확장된 점 계산
    x3 = x2 + dx_unit * width
    y3 = y2 + dy_unit * width

    return int(x3), int(y3)


# 3.초기 격자 검출 함수
# detect_red_squares(image_path) : 전체 이미지에서 빨간색 사각형 탐지
def detect_red_squares(image_path):
    """
    Detect red squares in the entire image.

    Parameters:
        image_path (str): Path to the image.

    Returns:
        list: Coordinates of the detected square.
    """
    image = load_image(image_path)
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

    # 빨간색 범위 정의
    lower_red1 = np.array([0, 100, 100])
    upper_red1 = np.array([10, 255, 255])
    lower_red2 = np.array([160, 100, 100])
    upper_red2 = np.array([180, 255, 255])

    # 빨간색 마스크 생성
    mask1 = cv2.inRange(hsv, lower_red1, upper_red1)
    mask2 = cv2.inRange(hsv, lower_red2, upper_red2)
    mask = cv2.bitwise_or(mask1, mask2)

    # 윤곽선 검출
    _, binary = cv2.threshold(mask, 50, 255, cv2.THRESH_BINARY)
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    detected_squares = []

    for cnt in contours:
        epsilon = 0.045 * cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, epsilon, True)

        # 사각형만 필터링 (꼭짓점 4개, 닫힌 컨투어)
        if len(approx) == 4 and cv2.isContourConvex(approx):
            area = cv2.contourArea(approx)
            if area > 500:  # 최소 면적 필터링
                detected_squares.append((approx, area))

    if not detected_squares:
        return None  # 사각형이 감지되지 않음

    # 가장 큰 사각형 반환
    detected_squares = sorted(detected_squares, key=lambda x: x[1], reverse=True)
    largest_square = detected_squares[0][0]  # 가장 큰 사각형의 좌표

    # 좌표 정렬
    sorted_square = sort_rectangle_points(largest_square)

    return sorted_square

# detect_red_squares_near_point(image_path, target_point, search_radius=50, click_mouse=False) : 특정 좌표 근처에서 빨간색 사각형 탐지
def detect_red_squares_near_point(image_path, target_point, click_mouse=False):
    """
    특정 좌표 근처에서 클릭 좌표의 색상에 기반한 사각형을 검출합니다.

    Parameters:
        image_path (str): 이미지 파일 경로
        target_point (tuple): 타겟 좌표 (x, y)
        click_mouse (bool): 클릭 기반 탐지 활성화 여부

    Returns:
        numpy.ndarray: 검출된 사각형의 꼭지점 좌표 (정렬됨) 또는 None
        str: 결과 메시지
    """
    if not click_mouse:
        return None, "Click-based detection is disabled."

    # 이미지 읽기
    image = cv2.imread(image_path)
    if image is None:
        return None, "이미지를 불러올 수 없습니다."

    # 이미지를 HSV 색 공간으로 변환
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)

    # 클릭 좌표에서의 HSV 값 추출
    x, y = target_point
    target_color = hsv[y, x]

    # HSV 범위 설정 (클릭 좌표 기반)
    hue_tolerance = 10
    saturation_tolerance = 50
    value_tolerance = 50

    lower_bound = np.array([
        max(0, target_color[0] - hue_tolerance),
        max(0, target_color[1] - saturation_tolerance),
        max(0, target_color[2] - value_tolerance)
    ])
    upper_bound = np.array([
        min(180, target_color[0] + hue_tolerance),
        min(255, target_color[1] + saturation_tolerance),
        min(255, target_color[2] + value_tolerance)
    ])

    # 마스크 생성
    mask = cv2.inRange(hsv, lower_bound, upper_bound)

    # 마스크 적용한 이미지
    masked_image = cv2.bitwise_and(image, image, mask=mask)

    # 그레이스케일 변환 및 이진화
    gray = cv2.cvtColor(masked_image, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 50, 255, cv2.THRESH_BINARY)

    # 윤곽선 찾기
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    closest_square = None
    min_distance = float('inf')

    for cnt in contours:
        # 윤곽선을 근사화하여 꼭짓점이 4개인 다각형 찾기
        epsilon = 0.045 * cv2.arcLength(cnt, True)
        approx = cv2.approxPolyDP(cnt, epsilon, True)

        if len(approx) == 4:  # 사각형 검출
            # 사각형 중심 계산
            M = cv2.moments(cnt)
            if M["m00"] == 0:  # 면적이 0인 경우 무시
                continue
            center_x = int(M["m10"] / M["m00"])
            center_y = int(M["m01"] / M["m00"])
            center = (center_x, center_y)

            # 클릭 좌표와 중심 간 거리 계산
            distance = np.sqrt((center[0] - target_point[0])**2 + (center[1] - target_point[1])**2)

            # 가장 가까운 사각형 선택
            if distance < min_distance:
                min_distance = distance
                closest_square = approx

    if closest_square is not None:
        # 좌표 정렬 적용
        sorted_square = sort_rectangle_points(closest_square)
        return sorted_square, None

    return None, "No red square detected near the specified point."





    


# 4.격자 확장 및 축소 함수
# extend(approx_list, direction, count, sort_direction) : 주어진 방향으로 격자 확장
def extend(approx_list, direction, count, sort_direction):
    """
    Generalized extend function to add rows or columns.

    Parameters:
        approx_list (list): The current grid.
        direction (str): The direction to extend ('up', 'down', 'left', 'right').
        count (int): The number of times to extend.
        sort_direction (str): The sort direction for the grid.

    Returns:
        list: Updated grid after extending.
    """
    for _ in range(count):
        if direction in ['up', 'down']:
            new_row = []
            base_row = approx_list[0] if direction == 'up' else approx_list[-1]
            for approx in base_row:
                if direction == 'up':
                    new_row.append(draw_up(approx))
                elif direction == 'down':
                    new_row.append(draw_down(approx))
            if direction == 'up':
                approx_list.insert(0, new_row)
            elif direction == 'down':
                approx_list.append(new_row)

        elif direction in ['left', 'right']:
            for row in approx_list:
                if direction == 'left':
                    row.insert(0, draw_left(row[0]))
                elif direction == 'right':
                    row.append(draw_right(row[-1]))

    return approx_list


# shrink(approx_list, direction) : 주어진 방향으로 격자 축소
def shrink(approx_list, direction):
    """
    Generalized shrink function to remove rows or columns.

    Parameters:
        approx_list (list): The current grid.
        direction (str): The direction to shrink ('up', 'down', 'left', 'right').

    Returns:
        list: Updated grid after shrinking.
    """
    if not approx_list:
        raise ValueError("Cannot shrink an empty grid.")

    if direction in ['up', 'down']:
        if len(approx_list) <= 1:
            raise ValueError("Cannot shrink further; only one row remains.")
        if direction == 'up':
            approx_list.pop(0)  # Remove the first row
        elif direction == 'down':
            approx_list.pop(-1)  # Remove the last row

    elif direction in ['left', 'right']:
        for row in approx_list:
            if len(row) <= 1:
                raise ValueError("Cannot shrink further; only one column remains.")
            if direction == 'left':
                row.pop(0)  # Remove the first column
            elif direction == 'right':
                row.pop(-1)  # Remove the last column

    return approx_list


# 5.격자 확장 방향별 함수

# draw_up() / draw_down() : 상단 및 하단 확장
# draw_up(approx) : 상단 방향으로 격자 확장
# 소실점 방향으로 격자 생성
# def draw_up(approx, vanishing_point_enabled):
#     """
#     Extend the grid upward. Consider vanishing point if enabled.

#     Parameters:
#         approx (numpy.ndarray): The rectangle coordinates.
#         vanishing_point_enabled (bool): Whether to consider the vanishing point.

#     Returns:
#         numpy.ndarray: Extended rectangle coordinates.
#     """
#     # 좌측 상단 및 하단 포인트 계산
#     left_start_point = approx[3][0]
#     left_end_point = approx[0][0]
#     left_distance = calculate_distance(left_start_point[0], left_start_point[1], left_end_point[0], left_end_point[1])

#     # 우측 상단 및 하단 포인트 계산
#     right_start_point = approx[2][0]
#     right_end_point = approx[1][0]
#     right_distance = calculate_distance(right_start_point[0], right_start_point[1], right_end_point[0], right_end_point[1])

#     if not vanishing_point_enabled:
#         # 소실점을 무시한 단순 확장
#         new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
#                                                     left_end_point[1], left_distance)
#         new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
#                                                      right_end_point[1], right_distance)
#         return np.array([
#             [[new_left_extend_point[0], new_left_extend_point[1]]],
#             [[new_right_extend_point[0], new_right_extend_point[1]]],
#             [[right_end_point[0], right_end_point[1]]],
#             [[left_end_point[0], left_end_point[1]]]
#         ], dtype=np.int32)

#     # 소실점을 고려한 확장
#     raw_distance = calculate_distance(left_end_point[0], left_end_point[1], right_end_point[0], right_end_point[1])
#     extend_distance = calculate_distance(left_start_point[0], left_start_point[1], right_start_point[0], right_start_point[1])
#     rate = extend_distance / raw_distance

#     # 소실점 기반 좌표 계산
#     new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
#                                                 left_end_point[1], int(left_distance * rate))
#     new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
#                                                  right_end_point[1], int(right_distance * rate))

#     return np.array([
#         [[new_left_extend_point[0], new_left_extend_point[1]]],
#         [[new_right_extend_point[0], new_right_extend_point[1]]],
#         [[right_end_point[0], right_end_point[1]]],
#         [[left_end_point[0], left_end_point[1]]]
#     ], dtype=np.int32)
def draw_up(approx):
    # print(approx)
    left_start_point = approx[3][0]
    left_end_point = approx[0][0]
    # print(left_start_point)
    # print(left_end_point)
    left_distance = calculate_distance(left_start_point[0], left_start_point[1], left_end_point[0], left_end_point[1])
    # print(left_distance)
    left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
                                            left_end_point[1], left_distance)
    # print(left_extend_point)
    right_start_point = approx[2][0]
    right_end_point = approx[1][0]
    # print(right_start_point)
    # print(right_end_point)
    right_distance = calculate_distance(right_start_point[0], right_start_point[1], right_end_point[0],
                                        right_end_point[1])
    right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
                                             right_end_point[1], right_distance)
    # print(right_extend_point)
    # 원래 폭
    raw_distance = calculate_distance(left_end_point[0], left_end_point[1], right_end_point[0], right_end_point[1])
    # print(raw_distance)
    # 축소 폭
    extend_distance = calculate_distance(left_extend_point[0], left_extend_point[1], right_extend_point[0],
                                         right_extend_point[1])
    # print(extend_distance)
    # 축소 비율
    rate = extend_distance / raw_distance
    # print(rate)
    # 축소 확장 좌표 계산
    new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
                                                left_end_point[1], int(left_distance * rate))
    new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
                                                 right_end_point[1], int(right_distance * rate))
    # print(new_left_extend_point)
    # print(new_right_extend_point)
    return np.array([[[new_left_extend_point[0], new_left_extend_point[1]]],
                     [[new_right_extend_point[0], new_right_extend_point[1]]],
                     [[right_end_point[0], right_end_point[1]]],
                     [[left_end_point[0], left_end_point[1]]]], dtype=np.int32)



# draw_down(approx) : 하단 방향으로 격자 확장
# 소실점 방향으로 격자 생성
# def draw_down(approx, vanishing_point_enabled):
#     """
#     Extend the grid downward. Consider vanishing point if enabled.

#     Parameters:
#         approx (numpy.ndarray): The rectangle coordinates.
#         vanishing_point_enabled (bool): Whether to consider the vanishing point.

#     Returns:
#         numpy.ndarray: Extended rectangle coordinates.
#     """
#     # 좌측 상단 및 하단 포인트 계산
#     left_start_point = approx[0][0]
#     left_end_point = approx[3][0]
#     left_distance = calculate_distance(left_start_point[0], left_start_point[1], left_end_point[0], left_end_point[1])

#     # 우측 상단 및 하단 포인트 계산
#     right_start_point = approx[1][0]
#     right_end_point = approx[2][0]
#     right_distance = calculate_distance(right_start_point[0], right_start_point[1], right_end_point[0], right_end_point[1])

#     if not vanishing_point_enabled:
#         # 소실점을 무시한 단순 확장
#         new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
#                                                     left_end_point[1], left_distance)
#         new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
#                                                      right_end_point[1], right_distance)
#         return np.array([
#             [[left_end_point[0], left_end_point[1]]],
#             [[right_end_point[0], right_end_point[1]]],
#             [[new_right_extend_point[0], new_right_extend_point[1]]],
#             [[new_left_extend_point[0], new_left_extend_point[1]]]
#         ], dtype=np.int32)

#     # 소실점을 고려한 확장
#     raw_distance = calculate_distance(left_end_point[0], left_end_point[1], right_end_point[0], right_end_point[1])
#     extend_distance = calculate_distance(left_start_point[0], left_start_point[1], right_start_point[0], right_start_point[1])
#     rate = extend_distance / raw_distance

#     # 소실점 기반 좌표 계산
#     new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
#                                                 left_end_point[1], int(left_distance * rate))
#     new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
#                                                  right_end_point[1], int(right_distance * rate))

#     return np.array([
#         [[left_end_point[0], left_end_point[1]]],
#         [[right_end_point[0], right_end_point[1]]],
#         [[new_right_extend_point[0], new_right_extend_point[1]]],
#         [[new_left_extend_point[0], new_left_extend_point[1]]]
#     ], dtype=np.int32)
def draw_down(approx):
    # print(approx)
    left_start_point = approx[0][0]
    left_end_point = approx[3][0]
    # print(left_start_point)
    # print(left_end_point)
    left_distance = calculate_distance(left_start_point[0], left_start_point[1], left_end_point[0], left_end_point[1])
    # print(left_distance)
    left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
                                            left_end_point[1], left_distance)
    # print(left_extend_point)
    right_start_point = approx[1][0]
    right_end_point = approx[2][0]
    # print(right_start_point)
    # print(right_end_point)
    right_distance = calculate_distance(right_start_point[0], right_start_point[1], right_end_point[0],
                                        right_end_point[1])
    right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
                                             right_end_point[1], right_distance)
    # print(right_extend_point)
    # 원래 폭
    raw_distance = calculate_distance(left_end_point[0], left_end_point[1], right_end_point[0], right_end_point[1])
    # print(raw_distance)
    # 축소 폭
    extend_distance = calculate_distance(left_extend_point[0], left_extend_point[1], right_extend_point[0],
                                         right_extend_point[1])
    # print(extend_distance)
    # 축소 비율
    rate = extend_distance / raw_distance
    # print(rate)
    # 축소 확장 좌표 계산
    new_left_extend_point = find_extended_point(left_start_point[0], left_start_point[1], left_end_point[0],
                                                left_end_point[1], int(left_distance * rate))
    new_right_extend_point = find_extended_point(right_start_point[0], right_start_point[1], right_end_point[0],
                                                 right_end_point[1], int(right_distance * rate))
    # print(new_left_extend_point)
    # print(new_right_extend_point)
    return np.array([[[left_end_point[0], left_end_point[1]]],
                     [[right_end_point[0], right_end_point[1]]],
                     [[new_right_extend_point[0], new_right_extend_point[1]]],
                     [[new_left_extend_point[0], new_left_extend_point[1]]]], dtype=np.int32)


# draw_left() / draw_right() : 왼쪽 및 오른쪽 확장
# draw_left(approx) : 왼쪽 방향으로 격자 확장
# 왼쪽 방향으로 격자 생성
def draw_left(approx):
    # print(approx)
    # 윗변 점
    up_start_point = approx[1][0]
    up_end_point = approx[0][0]
    # 윗변 길이
    up_distance = calculate_distance(up_start_point[0], up_start_point[1], up_end_point[0], up_end_point[1])
    up_extend_point = find_extended_point(up_start_point[0], up_start_point[1], up_end_point[0], up_end_point[1],
                                          int(up_distance))
    # 아래변 점
    down_start_point = approx[2][0]
    down_end_point = approx[3][0]
    # 아래변 길이
    down_distance = calculate_distance(down_start_point[0], down_start_point[1], down_end_point[0], down_end_point[1])
    down_extend_point = find_extended_point(down_start_point[0], down_start_point[1], down_end_point[0],
                                            down_end_point[1], int(down_distance))
    # print(up_extend_point)
    # print(down_extend_point)

    return np.array([[[up_extend_point[0], up_extend_point[1]]],
                     [[up_end_point[0], up_end_point[1]]],
                     [[down_end_point[0], down_end_point[1]]],
                     [[down_extend_point[0], down_extend_point[1]]]], dtype=np.int32)


# draw_right(approx) : 오른쪽 방향으로 격자 확장
# 오른쪽 방향으로 격자 생성
def draw_right(approx):
    # print(approx)
    # 윗변 점
    up_start_point = approx[0][0]
    up_end_point = approx[1][0]
    # 윗변 길이
    up_distance = calculate_distance(up_start_point[0], up_start_point[1], up_end_point[0], up_end_point[1])
    up_extend_point = find_extended_point(up_start_point[0], up_start_point[1], up_end_point[0], up_end_point[1],
                                          int(up_distance))
    # 아래변 점
    down_start_point = approx[3][0]
    down_end_point = approx[2][0]
    # 아래변 길이
    down_distance = calculate_distance(down_start_point[0], down_start_point[1], down_end_point[0], down_end_point[1])
    down_extend_point = find_extended_point(down_start_point[0], down_start_point[1], down_end_point[0],
                                            down_end_point[1], int(down_distance))
    # print(up_extend_point)
    # print(down_extend_point)

    return np.array([[[up_end_point[0], up_end_point[1]]],
                     [[up_extend_point[0], up_extend_point[1]]],
                     [[down_extend_point[0], down_extend_point[1]]],
                     [[down_end_point[0], down_end_point[1]]]], dtype=np.int32)


# up_extend(sort_direction, approx_list, count=1) : 상단 방향으로 여러 번 확장
# def up_extend(sort_direction, approx_list, count=1, vanishing_point_enabled=False):
def up_extend(sort_direction, approx_list, count=1):
    """
    Extend the grid upward multiple times.
    """
    for _ in range(count):
        local_approx_list = []
        if sort_direction == 'up':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[0]
            for approx in row:
                # new_row.append(draw_up(approx,vanishing_point_enabled))    
                new_row.append(draw_up(approx))
            local_approx_list.insert(0, new_row)

        elif sort_direction == 'down':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[-1]
            for approx in row:
                # new_row.append(draw_up(approx,vanishing_point_enabled))
                new_row.append(draw_up(approx))
            local_approx_list.append(new_row)

        elif sort_direction == 'left':
            for row in approx_list:
                new_row = row
                # new_row.append(draw_up(row[-1],vanishing_point_enabled))
                new_row.append(draw_up(row[-1]))
                local_approx_list.append(new_row)

        elif sort_direction == 'right':
            for row in approx_list:
                new_row = row
                # new_row.insert(0, draw_up(row[0],vanishing_point_enabled))
                new_row.insert(0, draw_up(row[0]))
                local_approx_list.append(new_row)
        approx_list = local_approx_list
    return approx_list


# down_extend(sort_direction, approx_list, count=1) : 하단 방향으로 여러 번 확장
# def down_extend(sort_direction, approx_list, count=1, vanishing_point_enabled=False):
def down_extend(sort_direction, approx_list, count=1):
    """
    Extend the grid downward multiple times.
    """
    for _ in range(count):
        local_approx_list = []
        if sort_direction == 'up':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[-1]
            for approx in row:
                # new_row.append(draw_down(approx,vanishing_point_enabled))
                new_row.append(draw_down(approx))
            local_approx_list.append(new_row)

        elif sort_direction == 'down':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[0]
            for approx in row:
                # new_row.append(draw_down(approx,vanishing_point_enabled))
                new_row.append(draw_down(approx))
            local_approx_list.insert(0, new_row)

        elif sort_direction == 'left':
            for row in approx_list:
                new_row = row
                # new_row.insert(0, draw_down(row[0],vanishing_point_enabled))
                new_row.insert(0, draw_down(row[0]))
                local_approx_list.append(new_row)

        elif sort_direction == 'right':
            for row in approx_list:
                new_row = row
                # new_row.append(draw_down(row[-1],vanishing_point_enabled))
                new_row.append(draw_down(row[-1]))
                local_approx_list.append(new_row)
        approx_list = local_approx_list
    return approx_list


# left_extend(sort_direction, approx_list, count=1) : 왼쪽 방향으로 여러 번 확장.
# def left_extend(sort_direction, approx_list, count=1, vanishing_point_enabled=False):
def left_extend(sort_direction, approx_list, count=1):
    """
    Extend the grid to the left multiple times.
    """
    for _ in range(count):
        local_approx_list = []
        if sort_direction == 'up':
            for row in approx_list:
                new_row = row
                new_row.insert(0, draw_left(row[0]))
                local_approx_list.append(new_row)

        elif sort_direction == 'down':
            for row in approx_list:
                new_row = row
                new_row.append(draw_left(row[-1]))
                local_approx_list.append(new_row)

        elif sort_direction == 'left':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[0]
            for approx in row:
                new_row.append(draw_left(approx))
            local_approx_list.insert(0, new_row)

        elif sort_direction == 'right':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[-1]
            for approx in row:
                new_row.append(draw_left(approx))
            local_approx_list.append(new_row)
        approx_list = local_approx_list
    return approx_list


# right_extend(sort_direction, approx_list, count=1) : 오른쪽 방향으로 여러 번 확장.
# def right_extend(sort_direction, approx_list, count=1, vanishing_point_enabled=False):
def right_extend(sort_direction, approx_list, count=1):
    """
    Extend the grid to the right multiple times.
    """
    for _ in range(count):
        local_approx_list = []
        if sort_direction == 'up':
            for row in approx_list:
                new_row = row
                new_row.append(draw_right(row[-1]))
                local_approx_list.append(new_row)

        elif sort_direction == 'down':
            for row in approx_list:
                new_row = row
                new_row.insert(0, draw_right(row[0]))
                local_approx_list.append(new_row)

        elif sort_direction == 'left':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[-1]
            for approx in row:
                new_row.append(draw_right(approx))
            local_approx_list.append(new_row)

        elif sort_direction == 'right':
            local_approx_list = approx_list
            new_row = []
            row = local_approx_list[0]
            for approx in row:
                new_row.append(draw_right(approx))
            local_approx_list.insert(0, new_row)
        approx_list = local_approx_list
    return approx_list


# 6.격자 축소 방향별 함수

# up_shrink(approx_list) : 상단 방향으로 축소
def up_shrink(approx_list, sort_direction):
    """
    Shrink the grid from the top (remove the first row).
    Ensures the grid does not shrink below the initial size.
    """
    if sort_direction == 'up':
        if len(approx_list) > 1:  # Ensure at least one row remains
            approx_list.pop(0)  # Remove the first row
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'down':
        if len(approx_list) > 1:  # Ensure at least one row remains
            approx_list.pop()  # Remove the first row
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'left':
        for row in approx_list:
            if len(row) > 1:
                row.pop()  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list
    elif sort_direction == 'right':
        for row in approx_list:
            if len(row) > 1:
                row.pop(0)  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list


# down_shrink(approx_list) : 하단 방향으로 축소
def down_shrink(approx_list, sort_direction):
    """
    Shrink the grid from the bottom (remove the last row).
    Ensures the grid does not shrink below the initial size.
    """
    if sort_direction == 'up':
        if len(approx_list) > 1:  # Ensure at least one row remains
            approx_list.pop(-1)  # Remove the last row
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'down':
        if len(approx_list) > 1:  # Ensure at least one row remains
            approx_list.pop(0)  # Remove the last row
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'left':
        local_approx_list = []
        for row in approx_list:
            if len(row) > 1:
                row.pop(0)  # Remove the first column
                local_approx_list.append(row)
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return local_approx_list

    elif sort_direction == 'right':
        local_approx_list = []
        for row in approx_list:
            if len(row) > 1:
                row.pop()  # Remove the first column
                local_approx_list.append(row)
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return local_approx_list


# left_shrink(approx_list) : 왼쪽 방향으로 축소
def left_shrink(approx_list, sort_direction):
    """
    Shrink the grid from the left (remove the first column of each row).
    Ensures the grid does not shrink below the initial size.
    """
    if sort_direction == 'up':
        for row in approx_list:
            if len(row) > 1:
                row.pop(0)  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list
    elif sort_direction == 'right':
        if len(approx_list) > 1:  # Ensure at least one row remains
            approx_list.pop(-1)  # Remove the last row
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'left':
        if len(approx_list) > 1:
            approx_list.pop(0)  # Remove the first column
        else:
            raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list
    elif sort_direction == 'down':
        for row in approx_list:
            if len(row) > 1:
                row.pop(-1)  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list


# right_shrink(approx_list) : 오른쪽 방향으로 축소
def right_shrink(approx_list, sort_direction):
    """
    Shrink the grid from the right (remove the last column of each row).
    Ensures the grid does not shrink below the initial size.
    """
    if sort_direction == 'up':
        for row in approx_list:
            if len(row) > 1:
                row.pop(-1)  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list
    elif sort_direction == 'right':
        if len(approx_list) > 1:
            approx_list.pop(0)  # Remove the first column
        else:
            raise ValueError("Cannot shrink further; only one row remains.")
        return approx_list
    elif sort_direction == 'left':
        if len(approx_list) > 1:
            approx_list.pop(-1)  # Remove the first column
        else:
            raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list
    elif sort_direction == 'down':
        for row in approx_list:
            if len(row) > 1:
                row.pop(0)  # Remove the first column
            else:
                raise ValueError("Cannot shrink further; only one column remains.")
        return approx_list


# 7.시각화

# show_approx(approx_list, img, sort_direction="up") : 격자를 이미지에 시각화
def show_approx(approx_list, img, sort_direction='up', display_labels=True):
    """
    Visualize the grid coordinates on the image with an option to disable displaying row and col labels.

    Parameters:
        approx_list (list): The current grid.
        img (numpy.ndarray): The image on which to display the grid.
        sort_direction (str): Direction of sorting ('up', 'down', 'left', 'right').
        display_labels (bool): Whether to display row and col labels on the image.
    """
    if not approx_list or len(approx_list[0]) == 0:
        print("Warning: approx_list is empty or invalid.")
        return img  # Return the original image without changes

    local_image = img.copy()
    rows = len(approx_list)
    cols = len(approx_list[0]) if rows > 0 else 0

    for index_row, row in enumerate(approx_list):
        if len(row) == 0:  # Skip empty rows
            continue

        for index_col, approx in enumerate(row):
            row_num, col_num = calculate_coordinates(index_row, index_col, rows, cols, sort_direction)

            # Draw the grid polygon
            cv2.polylines(local_image, [approx], isClosed=True, color=(0, 255, 0), thickness=3)
            for point in approx:
                cv2.circle(local_image, tuple(point[0]), 5, (255, 0, 0), -1)

            if display_labels:
                # Display adjusted row and col
                text = f'({row_num}, {col_num})'
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 0.7
                thickness = 2
                text_size = cv2.getTextSize(text, font, font_scale, thickness)[0]
                text_x = approx[0][0][0] + (approx[2][0][0] - approx[0][0][0] - text_size[0]) // 2
                text_y = approx[0][0][1] + (approx[2][0][1] - approx[0][0][1] + text_size[1]) // 2
                cv2.putText(local_image, text, (text_x, text_y), font, font_scale, color=(255, 255, 255),
                            thickness=thickness)

    return local_image


# 8. 파일 저장 및 로드

# save_output_image_data(image_base64, approx_list, sort_direction, grid_unit, initial_coordinates, unique_id, output_dir="output") :
# Base64 인코딩된 이미지와 격자 정보를 처리하여 격자 데이터를 시각화된 이미지 및 JSON 형식으로 저장
def save_output_image_data(image_base64, approx_list, sort_direction, grid_unit, initial_coordinates, unique_id,
                           output_dir="output"):
    """
    Save grid information, including unique_id, sort_direction, grid_unit, and initial_coordinates, into JSON and images.
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

        # Decode Base64 image and save the original image
        original_image_path = os.path.join(output_dir, "original_image.jpg")
        image = decode_base64_to_image(image_base64)
        cv2.imwrite(original_image_path, image)

        # Ensure all grid coordinates are numpy arrays
        processed_approx_list = []
        for row in approx_list:
            processed_row = []
            for cell in row:
                if isinstance(cell, np.ndarray):
                    processed_row.append(cell)
                else:
                    raise TypeError(f"Unexpected cell type: {type(cell)}")
            processed_approx_list.append(processed_row)

        # Save the grid visualization image
        grid_image_path = os.path.join(output_dir, "grid_image.jpg")
        output_image = show_approx(processed_approx_list, image)
        cv2.imwrite(grid_image_path, output_image)

        # Generate grid coordinates for JSON
        grid_coordinates = []
        for row_idx, row in enumerate(processed_approx_list):
            for col_idx, cell in enumerate(row):
                grid_coordinates.append({
                    "row": row_idx,
                    "col": col_idx,
                    "coordinates": cell.tolist()  # Convert numpy array to list
                })

        # Prepare JSON data
        json_data = {
            "unique_id": unique_id,  # Include unique_id
            "image_base64": image_base64,
            "grid_coordinates": grid_coordinates,
            "sort_direction": sort_direction,
            "grid_unit": grid_unit,
            "initial_coordinates": initial_coordinates
        }

        # Save JSON data
        json_path = os.path.join(output_dir, "grid_coordinates.json")
        with open(json_path, "w", encoding="utf-8") as json_file:
            json.dump(json_data, json_file, indent=4)

        return {
            "success": True,
            "message": "Grid state and image saved successfully.",
            "output_files": {
                "original_image": original_image_path,
                "grid_image": grid_image_path,
                "grid_coordinates": json_path
            }
        }

    except Exception as e:
        return {
            "success": False,
            "message": str(e)
        }


# load_grid_state(grid_coordinates_path) : JSON 파일에서 저장된 격자 상태를 로드
def load_grid_state(grid_coordinates_path):
    """
    JSON 파일에서 저장된 그리드 상태를 불러오는 함수.

    Parameters:
        grid_coordinates_path (str): JSON 파일 경로.

    Returns:
        tuple: (approx_list, sort_direction)
    """
    if not os.path.isfile(grid_coordinates_path):
        raise FileNotFoundError(f"Grid coordinates file not found: {grid_coordinates_path}")

    with open(grid_coordinates_path, 'r') as f:
        grid_data = json.load(f)

    if not isinstance(grid_data.get("grid_coordinates"), list):
        raise ValueError("The grid data must be a list of dictionaries.")

    # Extract sort_direction
    sort_direction = grid_data.get("sort_direction", "up")  # Default to "up" if not found

    # JSON 데이터를 approx_list 형태로 변환
    approx_list = []
    for item in grid_data["grid_coordinates"]:
        row = item["row"]
        col = item["col"]
        while len(approx_list) <= row:
            approx_list.append([])
        while len(approx_list[row]) <= col:
            approx_list[row].append(None)
        approx_list[row][col] = np.array(item["coordinates"], dtype=np.int32)

    return approx_list, sort_direction


# 9. 계산

# sort_rectangle_points(approx) : 감지된 사각형 좌표를 정렬
def sort_rectangle_points(approx):
    """
    Sort rectangle points in a consistent order: top-left, top-right, bottom-right, bottom-left.

    Parameters:
        approx (numpy.ndarray): Array of rectangle points (4 points).

    Returns:
        numpy.ndarray: Sorted rectangle points.
    """
    points = sorted(approx, key=lambda x: (x[0][1], x[0][0]))  # y값 우선 정렬
    top_points = sorted(points[:2], key=lambda x: x[0][0])  # 상단 좌표 (x값 기준)
    bottom_points = sorted(points[2:], key=lambda x: x[0][0])  # 하단 좌표 (x값 기준)
    return np.array([top_points[0], top_points[1], bottom_points[1], bottom_points[0]])


# calculate_coordinates(index_row, index_col, rows, cols, sort_direction) : 정렬 방향에 따라 행 및 열 좌표를 계산.
def calculate_coordinates(index_row, index_col, rows, cols, sort_direction):
    """
    Calculate row and column numbers based on the sort direction.
    """
    if sort_direction == 'up':
        # 행: 위에서 아래, 열: 좌에서 우
        return index_row, index_col
    elif sort_direction == 'down':
        # 행: 위에서 아래, 열: 좌에서 우
        return index_row, index_col
    elif sort_direction == 'left':
        # 행: 좌에서 우, 열: 위에서 아래
        return index_row, index_col
    elif sort_direction == 'right':
        # 행: 우에서 좌, 열: 위에서 아래
        return index_row, index_col


# generate_coordinates(approx_list, sort_direction) : 격자 좌표를 계산.
def generate_coordinates(approx_list, sort_direction):
    updated_coordinates = []
    rows = len(approx_list)
    cols = len(approx_list[0]) if rows > 0 else 0

    for index_row, row in enumerate(approx_list):
        for index_col, cell in enumerate(row):
            if sort_direction == 'up':
                row_num = index_row
                col_num = index_col
            elif sort_direction == 'down':
                row_num = rows - index_row - 1  # 아래에서 위로 증가
                col_num = index_col
            elif sort_direction == 'left':
                row_num = index_col
                col_num = rows - index_row - 1
            elif sort_direction == 'right':
                row_num = cols - index_col - 1
                col_num = index_row
            else:
                raise ValueError(f"Invalid sort direction: {sort_direction}")

            coordinates_tuple = tuple(map(tuple, cell.tolist())) if isinstance(cell, np.ndarray) else tuple(cell)
            updated_coordinates.append({
                "row": row_num,
                "col": col_num,
                "coordinates": coordinates_tuple
            })

    return updated_coordinates


# 안전관련 그리드 생성 함수

# 두점간 각도 계산 함수
def calculate_angle(point1, point2):
    """Calculates the angle (in degrees) between two points with respect to the x-axis."""
    delta_y = point2[1] - point1[1]
    delta_x = point2[0] - point1[0]
    angle = math.degrees(math.atan2(delta_y, delta_x))
    return angle

# 그리드 상태 (완성: True, 미완성: False)
def check_grid_row_consistency(grid):
    """Checks if all rows in the grid have the same number of elements."""
    if not grid:
        return True
    row_length = len(grid[0])
    return all(len(row) == row_length for row in grid)

# 두값을 비교 10%이상 차이 확인(이내: True, 초과: False)
def is_within_10_percent(value1, value2):
    """Returns True if the difference between two values is within 10% of the larger value."""
    max_value = max(value1, value2)
    return abs(value1 - value2) <= max_value * 0.1


'''
 point list를 grid에 추가하는 코드드

 gird: 사각형 저장대상 list
 point : 포인트 리스트
 point_buffer: 4개가 모이면 gird에 저장장
'''

def generate_grid(grid, point_buffer, point):
    """Generates a sequential grid by adding complete rectangles to the grid."""
    if len(grid) > 0:
        row = grid[-1]
    else:
        row = []

    point_buffer.append(point)

    if len(point_buffer) == 4:
        sort_point_buffer = sort_rectangle_points_1(point_buffer)
        row.append(sort_point_buffer.copy())
        point_buffer.clear()
    elif len(point_buffer) == 2:
        sort_point_buffer = sort_by_y(point_buffer)
        #마지막 사각형과 동일 y표 여부 확인
        if len(row) > 0:
            print('calculate_angle:::', calculate_angle(sort_point_buffer[0], sort_point_buffer[1]))
            last_rectangle = row[-1]
            # 상단 점 y
            up_y = last_rectangle[1][1]
            # 하단 점 y
            donw_y = last_rectangle[2][1]
            last_length = donw_y - up_y
            new_length = sort_point_buffer[1][1] - sort_point_buffer[0][1]
            if calculate_angle(sort_point_buffer[0], sort_point_buffer[1]) > 30 and is_within_10_percent(last_length, new_length):
                new_rectangle = []
                new_rectangle.append(last_rectangle[1])
                new_rectangle.append(sort_point_buffer[0])
                new_rectangle.append(sort_point_buffer[1])
                new_rectangle.append(last_rectangle[2])
                row.append(new_rectangle)
                point_buffer.clear()
            else:
                sort_point_buffer = sort_by_x(point_buffer)
                new_row = []
                new_rectangle = []
                new_rectangle.append(row[0][3])
                new_rectangle.append(row[0][2])
                new_rectangle.append(sort_point_buffer[1])
                new_rectangle.append(sort_point_buffer[0])
                new_row.append(new_rectangle)
                grid.append(new_row)
                point_buffer.clear()
                return grid    
    elif len(point_buffer) == 1:        
        print('len(grid)>1', len(grid)>1)
        print('len(row)>0', len(row)>0)
        if len(grid)>1 and len(row)>0:
            try:
                print('한점처리')
                up_rectangle = grid[-2][len(row)]
                new_rectangle = []
                new_rectangle.append(up_rectangle[3])
                new_rectangle.append(up_rectangle[2])
                new_rectangle.append(point_buffer[0])
                new_rectangle.append(row[-1][2])
                row.append(new_rectangle)
                point_buffer.clear()
            except Exception as e:
                print(e)
                pass
    if len(grid) > 0:
        grid[-1] = row
    else:
        grid.append(row)
    return grid


# 마지막 버퍼내용 그리드 추가가
def finalize_grid(grid, point_buffer):
    """Ensures the last incomplete buffer is added to the grid."""
    if point_buffer:
        try:
            row_idx = len(grid) // len(grid[0]) if grid else 0
        except Exception as e:
            print(e)
            row_idx = 0
        if len(grid) <= row_idx:
            grid.append([])
        grid[row_idx].append(point_buffer.copy())
        point_buffer.clear()

# 그리드 내용 이미지로 그리기기
def draw_grid_on_image(image, grid):
    """Draws a 2D grid of rectangles using pairs of points and labels the row and column indices."""
    for row_idx, row in enumerate(grid):
        for col_idx, rectangle in enumerate(row):
            if len(rectangle) == 4:
                points = np.array(rectangle, np.int32)
                cv2.polylines(image, [points], isClosed=True, color=(255, 0, 0), thickness=2)
                
                # Calculate center for labeling
                center_x = sum(p[0] for p in points) // 4
                center_y = sum(p[1] for p in points) // 4
                
                # Put row and column index in the center of the grid cell
                label = f"{row_idx},{col_idx}"
                cv2.putText(image, label, (center_x, center_y), cv2.FONT_HERSHEY_SIMPLEX, 
                            0.5, (255, 255, 255), 1, cv2.LINE_AA)
            
            # Draw points
            for p in rectangle:
                cv2.circle(image, tuple(p), 5, (0, 255, 0), -1)
    
    return image


# 그리드 정렬
def sort_grid(grid, order='up'):
    if order == 'up':
        return grid
    elif order == 'down':
        col_num = len(grid[0])
        new_grid = []
        flatten_grid = [rectangle for row in grid for rectangle in row]
        col_count = 0
        row = []
        while len(flatten_grid) > 0:
            if col_num > col_count:
                row.append(flatten_grid.pop())
                col_count += 1
            else :
                new_grid.append(row)
                row = []
                row.append(flatten_grid.pop())
                col_count = 1
        if len(row)>0:
            new_grid.append(row)
        return new_grid
    elif order == 'right':
        return [list(row) for row in zip(*grid)][::-1]
    elif order == 'left':
        return [list(row) for row in zip(*grid[::-1])]
    
def sort_rectangle_points_1(points):
    """
    네 개의 좌표를 좌상(TL), 우상(TR), 우하(BR), 좌하(BL) 순서로 정렬하는 함수.
    
    :param points: 네 개의 2D 좌표 리스트 [[x1, y1], [x2, y2], [x3, y3], [x4, y4]]
    :return: 정렬된 좌표 리스트 [[TL], [TR], [BR], [BL]]
    """
    # y 기준으로 오름차순 정렬 (위쪽 두 점과 아래쪽 두 점을 나누기 위해)
    points.sort(key=lambda p: p[1])

    # 위쪽 두 점 (y값이 작은 점들)
    top_points = points[:2]
    # 아래쪽 두 점 (y값이 큰 점들)
    bottom_points = points[2:]

    # x 기준으로 정렬하여 좌/우 구분
    top_points.sort(key=lambda p: p[0])  # 왼쪽이 TL, 오른쪽이 TR
    bottom_points.sort(key=lambda p: p[0])  # 왼쪽이 BL, 오른쪽이 BR

    # 정렬된 좌표 반환 (좌상, 우상, 우하, 좌하)
    return [top_points[0], top_points[1], bottom_points[1], bottom_points[0]]

def sort_by_y(points):
    """
    두 개의 좌표를 y값 기준으로 오름차순 정렬하는 함수.
    
    :param points: [[x1, y1], [x2, y2]] 형태의 리스트
    :return: y좌표 기준으로 정렬된 리스트
    """
    return sorted(points, key=lambda p: p[1])

def sort_by_x(points):
    """
    두 개의 좌표를 x값 기준으로 오름차순 정렬하는 함수.
    
    :param points: [[x1, y1], [x2, y2]] 형태의 리스트
    :return: x좌표 기준으로 정렬된 리스트
    """
    return sorted(points, key=lambda p: p[0])