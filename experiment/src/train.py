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
import json
import os
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


def get_next_run_name(models_dir: Path, base_name: str) -> str:
    """models 폴더를 확인해서 다음 넘버링된 실험 이름을 반환한다.

    Ultralytics와 동일한 넘버링 로직:
      exp_rtdetr_l → exp_rtdetr_l2 → exp_rtdetr_l3 → ...
    """
    if not (models_dir / base_name).exists():
        return base_name
    n = 2
    while (models_dir / f"{base_name}{n}").exists():
        n += 1
    return f"{base_name}{n}"


def update_mlflow_run_name(cfg: dict, run_id: str, new_name: str,
                           project_root: Path) -> None:
    """MLflow meta.yaml의 run_name을 직접 수정한다.

    end_run() 이후 호출해야 meta.yaml 덮어쓰기를 방지할 수 있다.
    """
    tracking_dir = (project_root / cfg["mlflow"]["tracking_uri"]).resolve()

    client = mlflow.tracking.MlflowClient()
    run_data = client.get_run(run_id)
    experiment_id = run_data.info.experiment_id

    # 1) meta.yaml의 run_name 직접 수정
    meta_path = tracking_dir / experiment_id / run_id / "meta.yaml"
    if meta_path.exists():
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = yaml.safe_load(f)
        meta["run_name"] = new_name
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(meta, f, default_flow_style=False, allow_unicode=True)

    # 2) mlflow.runName 태그 파일도 업데이트
    tag_path = tracking_dir / experiment_id / run_id / "tags" / "mlflow.runName"
    if tag_path.parent.exists():
        with open(tag_path, "w", encoding="utf-8") as f:
            f.write(new_name)


def train(config_path: str) -> None:
    cfg = load_config(config_path)
    project_root = Path(__file__).resolve().parent.parent

    setup_mlflow(cfg)

    arch = cfg["model"]["architecture"]
    model = RTDETR(arch) if "rtdetr" in arch.lower() else YOLO(arch)

    # 학습 전에 다음 넘버링 이름을 미리 계산
    models_dir = project_root / "models"
    base_name = cfg["experiment"]["name"]
    run_name = get_next_run_name(models_dir, base_name)
    print(f"[INFO] 실험 이름: {run_name}")

    # MLflow run 시작 (넘버링된 이름으로 시작)
    run = mlflow.start_run(run_name=run_name)
    run_id = run.info.run_id

    save_dir = None
    try:
        # 파라미터 기록
        log_params(cfg)
        mlflow.set_tags({
            "description": cfg["experiment"]["description"],
            "config_file": config_path,
        })

        # 학습 실행 (넘버링된 이름을 Ultralytics에도 전달)
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
            project=str(models_dir),
            name=run_name,
        )

        # Ultralytics가 run을 닫았으면 같은 run_id로 다시 열기
        if mlflow.active_run() is None:
            mlflow.start_run(run_id=run_id)

        # 실제 모델 저장 경로 (Ultralytics가 자동 넘버링한 경로)
        save_dir = Path(results.save_dir) if hasattr(results, "save_dir") else None

        # 메트릭 기록
        metrics_dict = {}
        if hasattr(results, "results_dict"):
            log_metrics(results.results_dict)
            metrics_dict = {k: float(v) for k, v in results.results_dict.items()}

        # best 모델 아티팩트 기록
        best_weight = save_dir / "weights" / "best.pt" if save_dir else None
        if best_weight and best_weight.exists():
            mlflow.log_artifact(str(best_weight), artifact_path="weights")

        # MLflow에 실제 모델 경로 태그 기록
        if save_dir:
            mlflow.set_tag("model_save_dir", str(save_dir))

        # 모델 폴더에 run_info.json 저장 (MLflow run_id 역추적용)
        if save_dir and save_dir.exists():
            run_info = {
                "mlflow_run_id": run_id,
                "experiment_name": save_dir.name,
                "description": cfg["experiment"]["description"],
                "config_file": config_path,
                "architecture": cfg["model"]["architecture"],
                "metrics": metrics_dict,
            }
            with open(save_dir / "run_info.json", "w", encoding="utf-8") as f:
                json.dump(run_info, f, indent=2, ensure_ascii=False)

        print(f"[INFO] 학습 완료. MLflow run: {run_id}")
        if save_dir:
            print(f"[INFO] 모델 저장: {save_dir}")

    finally:
        mlflow.end_run()

    # end_run 이후 meta.yaml 직접 수정 (end_run이 덮어쓰지 않도록)
    if save_dir:
        update_mlflow_run_name(cfg, run_id, save_dir.name, project_root)
        print(f"[INFO] MLflow run name 업데이트: {save_dir.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLOv11 학습 (MLflow 연동)")
    parser.add_argument(
        "--config", type=str, default="configs/default.yaml", help="실험 설정 파일"
    )
    args = parser.parse_args()
    train(args.config)
