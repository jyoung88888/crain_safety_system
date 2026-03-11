# dongyang-project

동양산전 안전관리시스템 - Object Detection 연구/학습 파이프라인

**PyTorch + Ultralytics (YOLOv11 / RT-DETR) + DVC + MLflow** 기반으로
데이터셋 버전 관리, 실험 추적, 서비스 연동 가능한 공통 모듈을 제공한다.

---

## 폴더 구조

```
dongyang-project/
├── configs/
│   ├── default.yaml          # YOLOv11 기본 실험 설정
│   └── exp_rtdetr.yaml       # RT-DETR 실험 설정
├── data/
│   ├── raw/                  # 원본 이미지 (DVC 추적)
│   ├── labels/               # YOLO txt 어노테이션 (DVC 추적)
│   ├── splits/               # dataset.py가 생성하는 train/val/test (자동)
│   └── dvc.yaml
├── models/                   # 학습 결과 weights (MLflow 아티팩트)
├── mlflow_runs/              # 로컬 MLflow 저장소
├── src/
│   ├── __init__.py
│   ├── dataset.py            # 데이터 분할 + dataset.yaml 자동 생성
│   ├── train.py              # 학습 래퍼 (MLflow 연동, 모델 종류 무관)
│   └── utils.py              # 후처리, bbox 유틸, 시각화 (서비스 공용)
├── dvc.yaml                  # DVC 파이프라인 정의
├── requirements.txt
├── .gitignore
└── README.md
```

### 각 파일 역할

| 파일 | 역할 |
|---|---|
| `configs/default.yaml` | YOLOv11 기본 설정. 모델, 학습, 증강, MLflow, 추론 파라미터 일괄 관리 |
| `configs/exp_rtdetr.yaml` | RT-DETR 실험 설정. Transformer 특성에 맞는 하이퍼파라미터 |
| `src/dataset.py` | `data/raw/` + `data/labels/` → train/val/test 분할, Ultralytics `dataset.yaml` 자동 생성, 클래스 분포 출력 |
| `src/train.py` | config 로드 → MLflow run 시작 → 학습 → 메트릭(mAP, precision, recall) + best.pt 아티팩트 기록 |
| `src/utils.py` | `Detection`/`FrameResult` 구조체, 결과 파싱, bbox 변환(xyxy↔xywh), IoU, 근접도 판별, 시각화 |
| `dvc.yaml` | `prepare` → `train` 2단계 파이프라인. `dvc repro` 한 번으로 전체 실행 |

---

## 지원 모델

Ultralytics 프레임워크를 통해 다양한 모델을 **config 변경만으로** 사용할 수 있다.

| 모델 | architecture 값 | 특징 |
|------|-----------------|------|
| YOLOv11 nano | `yolo11n.pt` | 가장 가벼움, 실시간 추론용 |
| YOLOv11 small | `yolo11s.pt` | 속도-정확도 균형 |
| YOLOv11 medium | `yolo11m.pt` | 범용 |
| YOLOv11 large | `yolo11l.pt` | 고정확도 |
| YOLOv11 xlarge | `yolo11x.pt` | 최고 정확도 |
| **RT-DETR-L** | **`rtdetr-l.pt`** | **Transformer 기반, NMS 불필요, 작은 객체 우수** |
| RT-DETR-X | `rtdetr-x.pt` | RT-DETR 고정확도 버전 |
| YOLOv8/v9/v10 | `yolov8n.pt` 등 | Ultralytics 하위 호환 |

### YOLOv11 vs RT-DETR 비교

| | YOLOv11 | RT-DETR |
|---|---|---|
| 구조 | CNN (anchor-free) | Transformer (end-to-end) |
| NMS | 필요 (iou_threshold 적용) | 불필요 (모델이 직접 중복 제거) |
| 작은 객체 | 보통 | 우수 (global attention) |
| VRAM | 적음 (batch 16 가능) | 많음 (batch 4 권장) |
| 학습 속도 | 빠름 | 느림 (warmup 길게 필요) |
| 권장 lr | 0.001 | 0.0001 (10배 낮게) |
| 권장 image_size | 640 | 1280 |

---

## 환경 설정

### 1. Python 환경

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

pip install -r requirements.txt
```

### 2. Git + DVC 초기화

```bash
git init
dvc init
```

### 3. (선택) DVC 리모트 설정

```bash
# 로컬 스토리지
dvc remote add -d local ./dvc_storage

# S3
# dvc remote add -d s3remote s3://bucket-name/path
```

---

## 사용 방법

### Step 1. 데이터 준비

원본 이미지와 YOLO 포맷 라벨을 배치한다.

```
data/
├── raw/
│   ├── img_001.jpg
│   ├── img_002.jpg
│   └── ...
└── labels/
    ├── img_001.txt       # class_id cx cy w h (normalized)
    ├── img_002.txt
    └── ...
```

라벨 포맷 (YOLO txt):
```
0 0.512 0.345 0.120 0.230    # person
1 0.231 0.456 0.050 0.060    # helmet
2 0.678 0.789 0.080 0.200    # hoist
```

클래스 ID 매핑:
| ID | 클래스 |
|----|--------|
| 0 | person |
| 1 | helmet |
| 2 | hoist |

### Step 2. DVC로 데이터 버전 관리

```bash
dvc add data/raw data/labels
git add data/raw.dvc data/labels.dvc data/.gitignore
git commit -m "데이터 v1: 초기 데이터셋 500장"
```

데이터가 추가/변경될 때마다:
```bash
# 이미지 추가 후
dvc add data/raw data/labels
git add data/raw.dvc data/labels.dvc
git commit -m "데이터 v2: 야간 이미지 200장 추가"
dvc push                      # 리모트에 업로드
```

### Step 3. 데이터셋 분할

```bash
python src/dataset.py --config configs/default.yaml
```

실행 결과:
```
[INFO] 총 500개 이미지-라벨 쌍 발견
[INFO] 데이터 분할 완료 → data/splits
  train: 400장
  val: 75장
  test: 25장
[INFO] dataset.yaml 생성 → data/dataset.yaml

[train] 클래스 분포:
  hoist: 412
  helmet: 1893
  person: 2105
```

### Step 4. 학습

```bash
# YOLOv11 학습
python src/train.py --config configs/default.yaml

# RT-DETR 학습
python src/train.py --config configs/exp_rtdetr.yaml
```

학습이 끝나면:
- `models/<experiment_name>/weights/best.pt` 파일이 로컬에 생성됨
- MLflow에 파라미터 + 메트릭 + 모델 아티팩트(`best.pt`)가 자동 기록됨
- 운영 기준 모델 소스는 MLflow를 우선으로 사용

### Step 5. MLflow UI로 실험 비교

```bash
mlflow ui --backend-store-uri ./mlflow_runs
# 브라우저에서 http://127.0.0.1:5000 접속
```

YOLOv11과 RT-DETR의 mAP, precision, recall을 나란히 비교 가능.

### (대안) DVC 파이프라인으로 한 번에 실행

Step 3 + Step 4를 한 번에:
```bash
dvc repro
```

단계별 실행:
```bash
dvc repro prepare    # 데이터 분할만
dvc repro train      # 학습만
```

---

## 실험 관리

### 새 실험 만들기

기존 config를 복사하고 원하는 값만 수정한다.

```bash
# YOLO 계열 실험
cp configs/default.yaml configs/exp_001_yolo11s.yaml

# RT-DETR 계열 실험
cp configs/exp_rtdetr.yaml configs/exp_002_rtdetr_x.yaml
```

수정 예시 (`exp_001_yolo11s.yaml`):
```yaml
experiment:
  name: "exp_001_yolo11s"
  description: "YOLOv11s 모델, mosaic off"

model:
  architecture: "yolo11s.pt"

augmentation:
  mosaic: 0.0
```

수정 예시 (`exp_002_rtdetr_x.yaml`):
```yaml
experiment:
  name: "exp_002_rtdetr_x"
  description: "RT-DETR-X 백본"

model:
  architecture: "rtdetr-x.pt"
```

실행:
```bash
python src/train.py --config configs/exp_001_yolo11s.yaml
python src/train.py --config configs/exp_002_rtdetr_x.yaml
```

### 실험 비교 (MLflow)

MLflow UI에서 실험별 mAP50, precision, recall을 그래프로 비교 가능.

### 실험 비교 (DVC)

```bash
dvc params diff                 # 파라미터 변경점 확인
dvc metrics diff                # 메트릭 변경점 확인
```

---

## 주요 설정값

### YOLOv11 (configs/default.yaml)

```yaml
model:
  architecture: "yolo11n.pt"
train:
  epochs: 100
  batch_size: 16
  lr0: 0.001
  patience: 20
augmentation:
  mosaic: 1.0
inference:
  conf_threshold: 0.5
  iou_threshold: 0.45
```

### RT-DETR (configs/exp_rtdetr.yaml)

```yaml
model:
  architecture: "rtdetr-l.pt"
data:
  image_size: 1280               # RT-DETR는 고해상도 권장
train:
  epochs: 1
  batch_size: 4                  # VRAM 절약
  lr0: 0.0001                    # Transformer는 낮은 lr
  warmup_epochs: 5               # 긴 warmup
  patience: 20
augmentation:
  mosaic: 0.0                    # RT-DETR에서는 off 권장
inference:
  conf_threshold: 0.5
  max_det: 300                   # Transformer query 수
```

전체 설정은 [configs/default.yaml](configs/default.yaml), [configs/exp_rtdetr.yaml](configs/exp_rtdetr.yaml) 참조.

---

## 서비스 연동

`src/utils.py`는 운영 코드와 공유 가능하도록 설계되었다.
YOLO, RT-DETR 모두 Ultralytics Results 객체를 반환하므로 동일한 후처리 코드를 사용한다.

```python
# 운영 코드에서 import 예시
from src.utils import parse_yolo_results, filter_by_class, compute_proximity

# 추론 결과 → 구조화 (YOLO, RT-DETR 모두 동일)
frame_result = parse_yolo_results(results[0], class_names=["person", "helmet", "hoist"])

# 클래스별 필터링
hoists = filter_by_class(frame_result, ["hoist"])
persons = filter_by_class(frame_result, ["person", "helmet"])

# 근접도 판별
for person in persons:
    for hoist in hoists:
        if compute_proximity(person, hoist, proximity_ratio=0.3):
            print(f"근접 감지: person {person.bbox} ↔ hoist {hoist.bbox}")
```

---

## 기술 스택

| 구분 | 도구 | 역할 |
|------|------|------|
| 모델 | Ultralytics YOLOv11 / RT-DETR | 객체 탐지 학습/추론 |
| 프레임워크 | PyTorch | 딥러닝 백엔드 |
| 데이터 버전 관리 | DVC | 이미지/라벨 버전 추적, 파이프라인 |
| 실험 추적 | MLflow | 하이퍼파라미터, 메트릭, 모델 아티팩트 |
| 설정 관리 | YAML | 실험별 config 분리 |
