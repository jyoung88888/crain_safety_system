"""
dataset.py - 데이터셋 준비 및 분할
====================================
역할:
  1. raw 이미지 + YOLO txt 라벨을 train/val/test로 분할
  2. Ultralytics용 dataset.yaml 자동 생성
  3. 데이터 통계 출력 (클래스 분포, 이미지 수)

사용 예:
  python src/dataset.py --config configs/default.yaml
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
from collections import Counter
from pathlib import Path

import yaml


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def collect_pairs(raw_dir: Path, label_dir: Path) -> list[tuple[Path, Path]]:
    """이미지-라벨 쌍을 수집한다. 라벨이 없는 이미지는 건너뛴다."""
    img_exts = {".jpg", ".jpeg", ".png", ".bmp"}
    pairs = []
    for img_path in sorted(raw_dir.iterdir()):
        if img_path.suffix.lower() not in img_exts:
            continue
        label_path = label_dir / f"{img_path.stem}.txt"
        if label_path.exists():
            pairs.append((img_path, label_path))
        else:
            print(f"[WARN] 라벨 없음, 건너뜀: {img_path.name}")
    return pairs


def split_dataset(
    pairs: list[tuple[Path, Path]],
    ratios: dict[str, float],
    seed: int = 42,
) -> dict[str, list[tuple[Path, Path]]]:
    """train / val / test로 분할한다."""
    random.seed(seed)
    random.shuffle(pairs)

    n = len(pairs)
    n_train = int(n * ratios["train"])
    n_val = int(n * ratios["val"])

    return {
        "train": pairs[:n_train],
        "val": pairs[n_train : n_train + n_val],
        "test": pairs[n_train + n_val :],
    }


def copy_split(
    split_data: dict[str, list[tuple[Path, Path]]],
    output_root: Path,
) -> None:
    """분할된 데이터를 images/ labels/ 하위로 복사한다."""
    for split_name, pairs in split_data.items():
        img_dir = output_root / split_name / "images"
        lbl_dir = output_root / split_name / "labels"
        img_dir.mkdir(parents=True, exist_ok=True)
        lbl_dir.mkdir(parents=True, exist_ok=True)

        for img_path, lbl_path in pairs:
            shutil.copy2(img_path, img_dir / img_path.name)
            shutil.copy2(lbl_path, lbl_dir / lbl_path.name)

    print(f"[INFO] 데이터 분할 완료 → {output_root}")
    for k, v in split_data.items():
        print(f"  {k}: {len(v)}장")


def generate_dataset_yaml(
    output_root: Path,
    class_names: list[str],
    yaml_path: Path,
) -> None:
    """Ultralytics 포맷의 dataset.yaml을 생성한다."""
    # 절대경로에 특수문자([], 한글 등)가 있으면 Ultralytics glob 매칭 실패
    # → yaml 파일 기준 상대경로 사용
    rel_path = os.path.relpath(output_root.resolve(), yaml_path.parent.resolve())
    dataset_cfg = {
        "path": rel_path,
        "train": "train/images",
        "val": "val/images",
        "test": "test/images",
        "nc": len(class_names),
        "names": class_names,
    }
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    with open(yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(dataset_cfg, f, default_flow_style=False, allow_unicode=True)
    print(f"[INFO] dataset.yaml 생성 → {yaml_path}")


def print_class_distribution(
    split_data: dict[str, list[tuple[Path, Path]]],
    class_names: list[str],
) -> None:
    """각 split별 클래스 분포를 출력한다."""
    for split_name, pairs in split_data.items():
        counter = Counter()
        for _, lbl_path in pairs:
            with open(lbl_path, "r") as f:
                for line in f:
                    parts = line.strip().split()
                    if not parts:
                        continue
                    cls_id = int(parts[0])
                    counter[cls_id] += 1
        print(f"\n[{split_name}] 클래스 분포:")
        for cls_id, name in enumerate(class_names):
            print(f"  {name}: {counter.get(cls_id, 0)}")


def prepare(config_path: str) -> None:
    cfg = load_config(config_path)
    project_root = Path(__file__).resolve().parent.parent

    raw_dir = project_root / "data" / "raw"
    label_dir = project_root / "data" / "labels"
    output_root = project_root / "data" / "splits"
    yaml_path = project_root / cfg["data"]["dataset_yaml"]

    pairs = collect_pairs(raw_dir, label_dir)
    if not pairs:
        print("[ERROR] 이미지-라벨 쌍이 없습니다. data/raw 와 data/labels 를 확인하세요.")
        return

    print(f"[INFO] 총 {len(pairs)}개 이미지-라벨 쌍 발견")

    split_data = split_dataset(pairs, cfg["data"]["split_ratio"], cfg["train"]["seed"])
    copy_split(split_data, output_root)
    generate_dataset_yaml(output_root, cfg["model"]["class_names"], yaml_path)
    print_class_distribution(split_data, cfg["model"]["class_names"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="데이터셋 분할 및 dataset.yaml 생성")
    parser.add_argument(
        "--config", type=str, default="configs/default.yaml", help="실험 설정 파일"
    )
    args = parser.parse_args()
    prepare(args.config)
