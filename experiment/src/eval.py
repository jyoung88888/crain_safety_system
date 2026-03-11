"""
eval.py - Test 데이터셋 평가 (MLflow 연동)
============================================
역할:
  1. 학습 완료된 모델(best.pt)로 test 데이터셋 평가
  2. 평가 메트릭을 기존 MLflow run에 test_ prefix로 기록
  3. run_info.json에 test_metrics 추가

사용 예:
  python src/eval.py -e exp_rtdetr_l2 --config configs/exp_rtdetr.yaml
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import mlflow
import yaml
from ultralytics import RTDETR, YOLO, settings as ultra_settings

# Ultralytics 내장 MLflow 콜백 비활성화 (우리 코드에서 직접 관리)
ultra_settings.update({"mlflow": False})


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_mlflow(cfg: dict) -> None:
    tracking_uri = Path(cfg["mlflow"]["tracking_uri"]).resolve().as_uri()
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(cfg["mlflow"]["experiment_name"])


def evaluate(experiment_name: str, config_path: str) -> None:
    cfg = load_config(config_path)
    project_root = Path(__file__).resolve().parent.parent

    # 모델 디렉토리 확인
    model_dir = project_root / "models" / experiment_name
    best_weight = model_dir / "weights" / "best.pt"
    run_info_path = model_dir / "run_info.json"

    if not best_weight.exists():
        print(f"[ERROR] 모델 가중치 없음: {best_weight}")
        return

    # run_info.json에서 MLflow run_id 읽기
    run_id = None
    run_info = {}
    if run_info_path.exists():
        with open(run_info_path, "r", encoding="utf-8") as f:
            run_info = json.load(f)
        run_id = run_info.get("mlflow_run_id")
        print(f"[INFO] MLflow run_id: {run_id}")
    else:
        print("[WARN] run_info.json 없음 — MLflow 기록 없이 평가만 진행")

    # 모델 로드 (RTDETR/YOLO 자동 분기)
    arch = cfg["model"]["architecture"]
    if "rtdetr" in arch.lower():
        model = RTDETR(str(best_weight))
    else:
        model = YOLO(str(best_weight))

    print(f"[INFO] 모델 로드: {best_weight}")
    print(f"[INFO] test 데이터셋 평가 시작...")

    # test 데이터셋 평가
    data_path = str(project_root / cfg["data"]["dataset_yaml"])
    results = model.val(
        data=data_path,
        split="test",
        imgsz=cfg["data"]["image_size"],
        batch=cfg["train"]["batch_size"],
        conf=cfg["inference"]["conf_threshold"],
        iou=cfg["inference"]["iou_threshold"],
        device=cfg["train"]["device"],
        workers=cfg["train"]["workers"],
    )

    # 메트릭 수집
    test_metrics = {}
    if hasattr(results, "results_dict"):
        metric_keys = [
            "metrics/precision(B)",
            "metrics/recall(B)",
            "metrics/mAP50(B)",
            "metrics/mAP50-95(B)",
        ]
        for key in metric_keys:
            if key in results.results_dict:
                test_metrics[key] = float(results.results_dict[key])

    # 결과 출력
    print("\n[결과] Test 데이터셋 평가:")
    for key, val in test_metrics.items():
        print(f"  {key}: {val:.4f}")

    # MLflow에 test 메트릭 기록 (기존 train run에 추가)
    if run_id:
        setup_mlflow(cfg)
        with mlflow.start_run(run_id=run_id):
            for key, val in test_metrics.items():
                safe_key = "test_" + key.replace("/", "_").replace("(", "").replace(")", "")
                mlflow.log_metric(safe_key, val)
            mlflow.set_tag("test_evaluated", "true")
        print(f"[INFO] MLflow에 test 메트릭 기록 완료 (run: {run_id})")

    # run_info.json에 test_metrics 추가
    if run_info_path.exists():
        run_info["test_metrics"] = test_metrics
        with open(run_info_path, "w", encoding="utf-8") as f:
            json.dump(run_info, f, indent=2, ensure_ascii=False)
        print(f"[INFO] run_info.json 업데이트 완료")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test 데이터셋 평가 (MLflow 연동)")
    parser.add_argument(
        "-e", "--experiment", type=str, required=True,
        help="실험 폴더명 (예: exp_rtdetr_l2)"
    )
    parser.add_argument(
        "--config", type=str, required=True,
        help="실험 설정 파일 (예: configs/exp_rtdetr.yaml)"
    )
    args = parser.parse_args()
    evaluate(args.experiment, args.config)
