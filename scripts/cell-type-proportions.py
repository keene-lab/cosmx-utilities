#!/usr/bin/env python3
"""
Generate a grouped bar chart comparing cell type proportions between two
CosMx slides. Uses the UMAP CSVs extracted by umap-cell-types.py.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ── Configuration ──────────────────────────────────────────────────────────────

SLIDE_1_CSV = "/tmp/seurat_slide1_umap.csv"
SLIDE_1_LABEL = "Contralateral uninvolved"
SLIDE_1_COLOR = "#4DBBD5"

SLIDE_2_CSV = "/tmp/seurat_slide2_umap.csv"
SLIDE_2_LABEL = "Normal brain"
SLIDE_2_COLOR = "#E64B35"
SLIDE_2_FOVS = set(range(1, 49)) | set(range(199, 201)) | set(range(148, 198))

OUTPUT_PATH = "cell-type-proportions.png"


def main():
    df1 = pd.read_csv(SLIDE_1_CSV)
    df1 = df1[df1["cell_type"].notna() & (df1["cell_type"] != "")]

    df2 = pd.read_csv(SLIDE_2_CSV)
    df2 = df2[df2["cell_type"].notna() & (df2["cell_type"] != "")]
    df2 = df2[df2["fov"].isin(SLIDE_2_FOVS)]

    # Compute proportions
    counts1 = df1["cell_type"].value_counts()
    counts2 = df2["cell_type"].value_counts()
    pct1 = counts1 / counts1.sum() * 100
    pct2 = counts2 / counts2.sum() * 100

    # Union of cell types, sorted alphabetically
    all_types = sorted(set(pct1.index) | set(pct2.index))

    props1 = [pct1.get(ct, 0) for ct in all_types]
    props2 = [pct2.get(ct, 0) for ct in all_types]

    # ── Plot ───────────────────────────────────────────────────────────────
    x = np.arange(len(all_types))
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))

    bars1 = ax.bar(x - bar_width / 2, props1, bar_width,
                   label=f"{SLIDE_1_LABEL} (n = {len(df1):,})",
                   color=SLIDE_1_COLOR, edgecolor="white", linewidth=0.5)
    bars2 = ax.bar(x + bar_width / 2, props2, bar_width,
                   label=f"{SLIDE_2_LABEL} (n = {len(df2):,})",
                   color=SLIDE_2_COLOR, edgecolor="white", linewidth=0.5)

    # Add percentage labels above bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            if height >= 0.5:
                ax.text(
                    bar.get_x() + bar.get_width() / 2, height + 0.3,
                    f"{height:.1f}%",
                    ha="center", va="bottom", fontsize=7,
                )

    ax.set_xticks(x)
    ax.set_xticklabels(all_types, rotation=45, ha="right", fontsize=9)
    ax.set_ylabel("% of cells", fontsize=11)
    ax.set_title("Cell type proportions by slide", fontsize=13)
    ax.legend(fontsize=10)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH, dpi=200, bbox_inches="tight")
    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
