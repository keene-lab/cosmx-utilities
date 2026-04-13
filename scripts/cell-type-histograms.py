#!/usr/bin/env python3
"""Generate cell type histograms for each CosMx slide from _metadata.csv files in S3.

Usage:
    uv run python scripts/cell-type-histograms.py \
        --bucket keene-cosmx-data \
        --prefix napari-stitched/CosMx-retina/CosMx-retina-brain-segmentation-test-4.1.26/Resegmentationcosmxretinabrain22626_01_04_2026_15_10_18_504 \
        --output-dir histograms
"""

import argparse
import csv
import io
import os
from collections import Counter

import boto3
import matplotlib.pyplot as plt


def list_slides(s3, bucket: str, prefix: str) -> list[str]:
    """List slide subdirectories under the given S3 prefix."""
    response = s3.list_objects_v2(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/", Delimiter="/",
    )
    return [
        p["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        for p in response.get("CommonPrefixes", [])
    ]


def load_metadata(s3, bucket: str, prefix: str, slide: str) -> tuple[Counter, dict]:
    """Load _metadata.csv for a slide and return (cell_type counts, color_map)."""
    key = f"{prefix.rstrip('/')}/{slide}/_metadata.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    counts = Counter()
    color_map = {}
    reader = csv.DictReader(io.StringIO(body))
    for row in reader:
        cell_type = row.get("cell_type", "").strip()
        if not cell_type:
            continue
        counts[cell_type] += 1
        if cell_type not in color_map:
            hex_color = row.get("hex_color", "").strip()
            if hex_color:
                color_map[cell_type] = hex_color

    return counts, color_map


def plot_histogram(
    slide: str, counts: Counter, color_map: dict, output_path: str,
) -> None:
    """Create and save a horizontal bar chart of cell type counts."""
    sorted_types = counts.most_common()
    labels = [ct for ct, _ in sorted_types]
    values = [n for _, n in sorted_types]
    colors = [color_map.get(ct, "#4C72B0") for ct in labels]

    fig_height = max(4, len(labels) * 0.35)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    ax.barh(labels, values, color=colors, edgecolor="white", linewidth=0.5)
    ax.invert_yaxis()
    ax.set_xlabel("Cell Count")
    ax.set_title(f"Cell Type Distribution — {slide}")
    ax.tick_params(axis="y", labelsize=8)

    for i, v in enumerate(values):
        ax.text(v + max(values) * 0.01, i, str(v), va="center", fontsize=7)

    plt.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate cell type histograms from _metadata.csv files in S3.",
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", required=True, help="S3 prefix containing slide directories")
    parser.add_argument("--output-dir", default="histograms", help="Local directory for output PNGs")
    args = parser.parse_args()

    s3 = boto3.client("s3")
    os.makedirs(args.output_dir, exist_ok=True)

    slides = list_slides(s3, args.bucket, args.prefix)
    print(f"Found {len(slides)} slides")

    for slide in sorted(slides):
        print(f"  {slide} ...", end=" ", flush=True)
        try:
            counts, color_map = load_metadata(s3, args.bucket, args.prefix, slide)
        except Exception as e:
            print(f"SKIPPED ({e})")
            continue

        output_path = os.path.join(args.output_dir, f"{slide}_cell_types.png")
        plot_histogram(slide, counts, color_map, output_path)
        print(f"{sum(counts.values())} cells, {len(counts)} types")

    print(f"\nHistograms saved to {args.output_dir}/")


if __name__ == "__main__":
    main()
