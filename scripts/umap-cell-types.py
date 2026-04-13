#!/usr/bin/env python3
"""
Generate UMAP plots colored by cell type for two CosMx slides.

1. Downloads Seurat RDS objects from S3
2. Uses R/Seurat to extract UMAP coordinates + cell type + FOV to CSV
3. Plots UMAPs in Python with matplotlib, labeled by cell type (with %)
"""

import os
import subprocess
import sys
import tempfile

import matplotlib.pyplot as plt
import matplotlib.patheffects as patheffects
import numpy as np
import pandas as pd
from adjustText import adjust_text

# ── Configuration ──────────────────────────────────────────────────────────────

SLIDE_1_S3 = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Contralateraluninvolved41026_11_04_2026_12_58_38_777/"
    "seuratObject_7522.A7.7582.A6.RDS"
)
SLIDE_1_LABEL = "Contralateral uninvolved"

SLIDE_2_S3 = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Normalbrain41026_11_04_2026_12_56_01_784/"
    "seuratObject_7674.A1.A4.A6.A7.RDS"
)
SLIDE_2_LABEL = "Normal brain"
SLIDE_2_FOVS = set(range(1, 49)) | set(range(199, 201)) | set(range(148, 198))

CELL_TYPE_COLUMN = "RNA_RNA_Cell.Typing.InSituType.Allen.Brain_1_clusters"
UMAP_REDUCTION = "RNA_UMAP.1_1"

OUTPUT_PATH = "umap-cell-types.png"

# ── Extract UMAP data from Seurat via R ────────────────────────────────────────

R_EXTRACT_SCRIPT = """
library(Seurat)
args <- commandArgs(trailingOnly = TRUE)
rds_path <- args[1]
out_path <- args[2]
cell_type_col <- args[3]
umap_name <- args[4]

cat("Loading Seurat object...\\n")
obj <- readRDS(rds_path)

umap_coords <- Embeddings(obj, reduction = umap_name)
meta <- obj@meta.data

df <- data.frame(
    UMAP_1 = umap_coords[, 1],
    UMAP_2 = umap_coords[, 2],
    cell_type = meta[[cell_type_col]],
    fov = meta$fov,
    stringsAsFactors = FALSE
)

write.csv(df, out_path, row.names = FALSE)
cat("Wrote", nrow(df), "cells to", out_path, "\\n")
"""


def download_from_s3(s3_path, local_path):
    """Download a file from S3 if not already cached locally."""
    if os.path.exists(local_path):
        print(f"  Using cached {local_path}")
        return
    print(f"  Downloading {s3_path.split('/')[-1]} ...")
    result = subprocess.run(
        ["aws", "s3", "cp", s3_path, local_path],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"Error: {result.stderr.decode()}", file=sys.stderr)
        sys.exit(1)


def extract_umap_csv(rds_path, csv_path):
    """Run R script to extract UMAP + metadata from a Seurat RDS."""
    print(f"  Extracting UMAP coordinates with R ...")
    with tempfile.NamedTemporaryFile(mode="w", suffix=".R", delete=False) as f:
        f.write(R_EXTRACT_SCRIPT)
        r_script_path = f.name

    result = subprocess.run(
        ["Rscript", r_script_path, rds_path, csv_path, CELL_TYPE_COLUMN, UMAP_REDUCTION],
        capture_output=True, text=True,
    )
    os.unlink(r_script_path)

    if result.returncode != 0:
        print(f"R error:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"  {result.stdout.strip()}")


def load_slide_umap(s3_path, label, local_name, fov_filter=None):
    """Download Seurat RDS, extract UMAP CSV, load and optionally filter."""
    rds_local = os.path.join("/tmp", local_name)
    csv_local = rds_local.replace(".RDS", "_umap.csv")

    download_from_s3(s3_path, rds_local)

    if not os.path.exists(csv_local):
        extract_umap_csv(rds_local, csv_local)
    else:
        print(f"  Using cached {csv_local}")

    df = pd.read_csv(csv_local)
    # Drop cells with no cell type annotation
    df = df[df["cell_type"].notna() & (df["cell_type"] != "")]

    if fov_filter is not None:
        df = df[df["fov"].isin(fov_filter)]

    print(f"  {label}: {len(df)} cells, {df['cell_type'].nunique()} cell types")
    return df


# ── Plotting ───────────────────────────────────────────────────────────────────

# Colorblind-friendly palette (expanded)
PALETTE = [
    "#E64B35", "#4DBBD5", "#00A087", "#3C5488", "#F39B7F",
    "#8491B4", "#91D1C2", "#DC9400", "#7E6148", "#B09C85",
    "#E7298A", "#66A61E", "#E6AB02", "#A6761D", "#1B9E77",
    "#D95F02", "#7570B3", "#E7298A", "#A6CEE3", "#1F78B4",
]


def assign_colors(cell_types):
    """Assign a consistent color to each cell type."""
    return {ct: PALETTE[i % len(PALETTE)] for i, ct in enumerate(sorted(cell_types))}


def plot_umap(ax, df, color_map, title):
    """Plot a single UMAP panel with direct cluster labels."""
    total_cells = len(df)

    # Shuffle points so no cell type is always on top
    df = df.sample(frac=1, random_state=42)

    for ct in sorted(df["cell_type"].unique()):
        mask = df["cell_type"] == ct
        ax.scatter(
            df.loc[mask, "UMAP_1"],
            df.loc[mask, "UMAP_2"],
            c=color_map[ct],
            s=1,
            alpha=0.3,
            rasterized=True,
        )

    # Direct labels at cluster centroids (like Seurat's LabelClusters)
    text_outline = [
        patheffects.Stroke(linewidth=3, foreground="white"),
        patheffects.Normal(),
    ]
    texts = []
    for ct in sorted(df["cell_type"].unique()):
        subset = df[df["cell_type"] == ct]
        centroid_x = subset["UMAP_1"].median()
        centroid_y = subset["UMAP_2"].median()
        n = len(subset)
        pct = n / total_cells * 100
        label = f"{ct} ({pct:.1f}%)"
        t = ax.text(
            centroid_x, centroid_y, label,
            fontsize=8,
            fontweight="bold",
            ha="center", va="center",
            color=color_map[ct],
            path_effects=text_outline,
        )
        texts.append(t)

    adjust_text(
        texts, ax=ax,
        expand=(1.5, 1.5),
        arrowprops=dict(arrowstyle="-", color="grey", lw=0.5),
    )

    ax.set_title(title, fontsize=12)
    ax.set_xlabel("UMAP 1", fontsize=9)
    ax.set_ylabel("UMAP 2", fontsize=9)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Slide 1 (all FOVs) ...")
    df1 = load_slide_umap(SLIDE_1_S3, SLIDE_1_LABEL, "seurat_slide1.RDS")

    print("Slide 2 (filtered FOVs) ...")
    df2 = load_slide_umap(SLIDE_2_S3, SLIDE_2_LABEL, "seurat_slide2.RDS", fov_filter=SLIDE_2_FOVS)

    # Use a shared color map across both slides
    all_types = set(df1["cell_type"].unique()) | set(df2["cell_type"].unique())
    color_map = assign_colors(all_types)

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    fig.subplots_adjust(wspace=0.2)

    plot_umap(axes[0], df1, color_map, SLIDE_1_LABEL)
    plot_umap(axes[1], df2, color_map, SLIDE_2_LABEL)

    fig.suptitle("UMAP by cell type", fontsize=14, y=1.02)
    plt.savefig(OUTPUT_PATH, dpi=200, bbox_inches="tight")
    print(f"\nSaved UMAP plot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
