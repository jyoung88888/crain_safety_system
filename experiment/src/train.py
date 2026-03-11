"""
train.py - YOLOv11 학습 래퍼 (MLflow 연동)
=============================================
역할:
  1. config YAML 로드
  2. MLflow run 시작 → 파라미터 기록
  3. Ultralytics YOLO 학습 실행
  4. 메트릭/아티팩트(모델 weights) MLflow에 기록

사용 예:
  python src/train.py --config configs/default.yaml
  python src/train.py --config configs/exp_001_aug.yaml
"""

from __future__ import annotations

import argparse
from pathlib import Path

import mlflow
import yaml
from ultralytics import RTDETR, YOLO


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_mlflow(cfg: dict) -> None:
    """MLflow tracking URI와 experiment를 설정한다."""
    tracking_uri = Path(cfg["mlflow"]["tracking_uri"]).resolve().as_uri()
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(cfg["mlflow"]["experiment_name"])


def log_params(cfg: dict) -> None:
    """config의 주요 파라미터를 MLflow에 기록한다."""
    mlflow.log_param("model_architecture", cfg["model"]["architecture"])
    mlflow.log_param("num_classes", cfg["model"]["num_classes"])
    mlflow.log_param("image_size", cfg["data"]["image_size"])

    for key, val in cfg["train"].items():
        mlflow.log_param(f"train_{key}", val)

    for key, val in cfg["augmentation"].items():
        mlflow.log_param(f"aug_{key}", val)

    mlflow.log_param("conf_threshold", cfg["inference"]["conf_threshold"])
    mlflow.log_param("iou_threshold", cfg["inference"]["iou_threshold"])


def log_metrics(metrics: dict) -> None:
    """학습 결과 메트릭을 MLflow에 기록한다."""
    metric_keys = [
        "metrics/precision(B)",
        "metrics/recall(B)",
        "metrics/mAP50(B)",
        "metrics/mAP50-95(B)",
    ]
    for key in metric_keys:
        if key in metrics:
            safe_key = key.replace("/", "_").replace("(", "").replace(")", "")
            mlflow.log_metric(safe_key, float(metrics[key]))


def train(config_path: str) -> None:
    cfg = load_config(config_path)
    project_root = Path(__file__).resolve().parent.parent

    setup_mlflow(cfg)

    arch = cfg["model"]["architecture"]
    model = RTDETR(arch) if "rtdetr" in arch.lower() else YOLO(arch)

    with mlflow.start_run(run_name=cfg["experiment"]["name"]):
        # 파라미터 기록
        log_params(cfg)
        mlflow.set_tags({
            "description": cfg["experiment"]["description"],
            "config_file": config_path,
        })

        # 학습 실행
        results = model.train(
            data=str(project_root / cfg["data"]["dataset_yaml"]),
            epochs=cfg["train"]["epochs"],
            batch=cfg["train"]["batch_size"],
            imgsz=cfg["data"]["image_size"],
            optimizer=cfg["train"]["optimizer"],
            lr0=cfg["train"]["lr0"],
            lrf=cfg["train"]["lrf"],
            momentum=cfg["train"]["momentum"],
            weight_decay=cfg["train"]["weight_decay"],
            warmup_epochs=cfg["train"]["warmup_epochs"],
            patience=cfg["train"]["patience"],
            device=cfg["train"]["device"],
            workers=cfg["train"]["workers"],
            seed=cfg["train"]["seed"],
            # 증강
            hsv_h=cfg["augmentation"]["hsv_h"],
            hsv_s=cfg["augmentation"]["hsv_s"],
            hsv_v=cfg["augmentation"]["hsv_v"],
            degrees=cfg["augmentation"]["degrees"],
            translate=cfg["augmentation"]["translate"],
            scale=cfg["augmentation"]["scale"],
            fliplr=cfg["augmentation"]["fliplr"],
            mosaic=cfg["augmentation"]["mosaic"],
            mixup=cfg["augmentation"]["mixup"],
            project=str(project_root / "models"),
            name=cfg["experiment"]["name"],
        )

        # 메트릭 기록
        if hasattr(results, "results_dict"):
            log_metrics(results.results_dict)

        # best 모델 아티팩트 기록
        best_weight = (
            project_root
            / "models"
            / cfg["experiment"]["name"]
            / "weights"
            / "best.pt"
        )
        if best_weight.exists():
            mlflow.log_artifact(str(best_weight), artifact_path="weights")

        print(f"[INFO] 학습 완료. MLflow run: {mlflow.active_run().info.run_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLOv11 학습 (MLflow 연동)")
    parser.add_argument(
        "--config", type=str, default="configs/default.yaml", help="실험 설정 파일"
    )
    args = parser.parse_args()
    train(args.config)
