import os
from pathlib import Path

# python-dotenv가 있으면 .env 파일 로드, 없으면 docker-compose env_file로 대체
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

class Config:
    APP_HOST = os.getenv("APP_HOST", "192.168.3.37")

    FLASK_PORT = int(os.getenv("FLASK_PORT", "8088"))

    DB_HOST = os.getenv("DB_HOST", "192.168.0.13")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_DATABASE = os.getenv("DB_DATABASE", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "!Gns@ulsan")

    RTMP_HOST= os.getenv("RTMP_HOST", "192.168.0.13")
    RTMP_PORT= int(os.getenv("RTMP_PORT", "1935"))

    REDIS_HOST = os.getenv("REDIS_HOST", "192.168.0.13")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")

    CCTV_IP = os.getenv("CCTV_IP", "192.168.3.201")

    # AI Detection 설정
    IOU_THRESHOLD = 0.05        # Person-Helmet IOU 매칭 기준
    CONF_THRESHOLD = 0.65       # 신뢰도 기준
    PERSON_ROI_OVERLAP_MIN = 0.1  # ROI∩person_bbox / person_bbox 최소 비율

    # 호이스트 이동 감지 설정
    HOIST_WINDOW = 10           # 누적 이동 거리 계산에 사용할 최근 프레임 수
    HOIST_MOVING_RATIO = 0.2  # bbox 대비 누적 이동 비율 >= 이 값이면 "이동 중"
    HOIST_STOP_RATIO = 0.2    # bbox 대비 누적 이동 비율 < 이 값이면 "정지"
    HOIST_WORKING_DURATION = 2.0  # IDLE → WORKING 전환에 필요한 지속 시간 (초)
    HOIST_IDLE_DURATION = 3.0     # WORKING → IDLE 전환에 필요한 지속 시간 (초)
    HOIST_MAX_JUMP = 150        # 프레임 간 점프 감지 임계값(px)
    HOIST_NOISE_GATE = 3        # 프레임 간 이동이 이 값(px) 이하면 노이즈로 무시

    # 안전 조건 설정
    SAFETY_MIN_PEOPLE = 2       # 호이스트 작동 시 최소 인원
    SAFETY_MIN_HELMETS = 2      # 호이스트 작동 시 최소 헬멧 착용 인원
    HOIST_PROXIMITY_RADIUS = 200  # 호이스트 근접 인원 판별 반경 (px)
    ZONE_COUNT_WINDOW = 1.0       # 인원 수 안정화 윈도우 (초) — 최근 N초 중 max
    SAFETY_DANGER_DURATION = 3.0  # SAFE→DANGER 전환에 필요한 지속 시간 (초)
    EVENT_MIN_INTERVAL = 120      # 이벤트 최소 발생 간격 (초)

    LATEST_JPEG_TTL_SEC = 2         # redis 키 만료 시간 
    LATEST_JPEG_INTERVAL_SEC = 1.0  # jpeg를 redis에 쓰는 최소 간격 
    LATEST_JPEG_QUALITY = 85        # 앞축 품질 

    ZONE_COLORS = {
        1: (255, 0, 0),    # 파랑 (BGR)
        2: (0, 0, 255),    # 빨강 (BGR)
        3: (0, 255, 0),    # 초록 (BGR)
        4: (255, 255, 0),  # 시안 (BGR)
    }
    DEFAULT_ZONE_COLOR = (128, 128, 128)