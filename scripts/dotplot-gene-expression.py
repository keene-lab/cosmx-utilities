#!/usr/bin/env python3
"""
Generate a single-cell RNA-seq style dot plot comparing gene expression
across cell types between two CosMx slides.

Dot size  = fraction of cells expressing the gene (count > 0)
Dot color = mean expression (transcript count) among expressing cells
"""

import gzip
import io
import subprocess
import sys

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────────

GENES = ["PSAP", "SCG5", "ITM2C", "GRIA1", "GRIA3", "GRM5"]

SLIDE_1_EXPR = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Contralateraluninvolved41026_11_04_2026_12_58_38_777/"
    "flatFiles/7522A77582A6/7522A77582A6_exprMat_file.csv.gz"
)
SLIDE_1_META = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Contralateraluninvolved41026_11_04_2026_12_58_38_777/"
    "flatFiles/7522A77582A6/7522A77582A6_metadata_file.csv.gz"
)
SLIDE_1_LABEL = "Contralateral uninvolved"

SLIDE_2_EXPR = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Normalbrain41026_11_04_2026_12_56_01_784/"
    "flatFiles/7674A1A4A6A7/7674A1A4A6A7_exprMat_file.csv.gz"
)
SLIDE_2_META = (
    "s3://keene-cosmx-data/CosMx-GBM/"
    "Contralateral-uninvolved-normal-brain-4.11.26/"
    "Normalbrain41026_11_04_2026_12_56_01_784/"
    "flatFiles/7674A1A4A6A7/7674A1A4A6A7_metadata_file.csv.gz"
)
SLIDE_2_LABEL = "Normal brain"
SLIDE_2_FOVS = set(range(1, 49)) | set(range(199, 201)) | set(range(148, 198))

CELL_TYPE_COLUMN = "RNA_RNA_Cell.Typing.InSituType.Allen.Brain_1_clusters"

OUTPUT_PATH = "dotplot-gene-expression.png"


# ── Helpers ────────────────────────────────────────────────────────────────────

def read_s3_gzipped_csv(s3_path, usecols=None):
    """Download a gzipped CSV from S3 and return a DataFrame."""
    print(f"  Downloading {s3_path.split('/')[-1]} ...")
    result = subprocess.run(
        ["aws", "s3", "cp", s3_path, "-"],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"Error downloading {s3_path}: {result.stderr.decode()}", file=sys.stderr)
        sys.exit(1)
    with gzip.open(io.BytesIO(result.stdout), "rt") as f:
        return pd.read_csv(f, usecols=usecols)


def load_slide(expr_path, meta_path, fov_filter=None):
    """Load expression + metadata for one slide, optionally filtering FOVs."""
    expr_cols = ["fov", "cell_ID"] + GENES
    df_expr = read_s3_gzipped_csv(expr_path, usecols=expr_cols)

    meta_cols = ["fov", "cell_ID", CELL_TYPE_COLUMN]
    df_meta = read_s3_gzipped_csv(meta_path, usecols=meta_cols)

    if fov_filter is not None:
        df_expr = df_expr[df_expr["fov"].isin(fov_filter)]
        df_meta = df_meta[df_meta["fov"].isin(fov_filter)]

    df = df_expr.merge(df_meta[["fov", "cell_ID", CELL_TYPE_COLUMN]], on=["fov", "cell_ID"])
    df = df.rename(columns={CELL_TYPE_COLUMN: "cell_type"})
    # Drop cells with no cell type annotation
    df = df[df["cell_type"].notna() & (df["cell_type"] != "")]
    return df


def compute_dotplot_stats(df):
    """
    For each cell_type × gene, compute:
      - pct_expressed: fraction of cells with count > 0
      - mean_expression: mean transcript count across ALL cells in that group
    Returns a DataFrame with columns: cell_type, gene, pct_expressed, mean_expression
    """
    rows = []
    for cell_type, group in df.groupby("cell_type"):
        for gene in GENES:
            counts = group[gene]
            n_cells = len(counts)
            n_expressing = (counts > 0).sum()
            pct = n_expressing / n_cells if n_cells > 0 else 0
            mean_expr = counts.mean() if n_cells > 0 else 0
            rows.append({
                "cell_type": cell_type,
                "gene": gene,
                "pct_expressed": pct,
                "mean_expression": mean_expr,
            })
    return pd.DataFrame(rows)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Loading Slide 1 (all FOVs) ...")
    df1 = load_slide(SLIDE_1_EXPR, SLIDE_1_META)
    print(f"  {len(df1)} cells, {df1['cell_type'].nunique()} cell types")

    print("Loading Slide 2 (filtered FOVs) ...")
    df2 = load_slide(SLIDE_2_EXPR, SLIDE_2_META, fov_filter=SLIDE_2_FOVS)
    print(f"  {len(df2)} cells, {df2['cell_type'].nunique()} cell types")

    stats1 = compute_dotplot_stats(df1)
    stats2 = compute_dotplot_stats(df2)

    # Use cell types present in BOTH slides, sorted alphabetically
    shared_types = sorted(
        set(stats1["cell_type"].unique()) & set(stats2["cell_type"].unique())
    )
    stats1 = stats1[stats1["cell_type"].isin(shared_types)]
    stats2 = stats2[stats2["cell_type"].isin(shared_types)]

    # ── Build the dot plot ─────────────────────────────────────────────────
    n_types = len(shared_types)
    n_genes = len(GENES)

    # Shared color scale across both panels
    vmin = 0
    vmax = max(stats1["mean_expression"].max(), stats2["mean_expression"].max())
    cmap = plt.cm.Reds

    fig, axes = plt.subplots(
        1, 2,
        figsize=(n_types * 0.9 + 4, n_genes * 0.8 + 2),
        sharey=True,
    )

    for ax, stats, label in [
        (axes[0], stats1, SLIDE_1_LABEL),
        (axes[1], stats2, SLIDE_2_LABEL),
    ]:
        for i, gene in enumerate(GENES):
            for j, ct in enumerate(shared_types):
                row = stats[(stats["gene"] == gene) & (stats["cell_type"] == ct)]
                if row.empty:
                    continue
                pct = row["pct_expressed"].values[0]
                expr = row["mean_expression"].values[0]
                # Dot size proportional to percent expressed
                size = pct * 200  # scale factor
                color = cmap(mcolors.Normalize(vmin=vmin, vmax=vmax)(expr))
                ax.scatter(j, i, s=size, c=[color], edgecolors="grey", linewidths=0.5)

        ax.set_xticks(range(n_types))
        ax.set_xticklabels(shared_types, rotation=90, ha="center", fontsize=8)
        ax.set_title(label, fontsize=10, pad=10)
        ax.set_xlim(-0.5, n_types - 0.5)
        ax.set_ylim(-0.5, n_genes - 0.5)
        ax.invert_yaxis()

    axes[0].set_yticks(range(n_genes))
    axes[0].set_yticklabels(GENES, fontsize=9)

    # ── Legends ────────────────────────────────────────────────────────────
    fig.subplots_adjust(right=0.82)

    # Color bar for mean expression
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=mcolors.Normalize(vmin=vmin, vmax=vmax))
    sm.set_array([])
    cbar_ax = fig.add_axes([0.85, 0.15, 0.02, 0.30])
    cbar = fig.colorbar(sm, cax=cbar_ax)
    cbar.set_label("Mean expression\n(transcripts)", fontsize=9)

    # Size legend for percent expressed
    legend_pcts = [0.25, 0.50, 0.75, 1.00]
    legend_handles = [
        plt.scatter([], [], s=p * 200, c="grey", alpha=0.5, edgecolors="grey", linewidths=0.5)
        for p in legend_pcts
    ]
    legend_labels = [f"{int(p * 100)}%" for p in legend_pcts]
    fig.legend(
        legend_handles, legend_labels,
        title="% expressing",
        loc="upper left",
        bbox_to_anchor=(0.85, 0.92),
        frameon=False,
        fontsize=9,
        title_fontsize=9,
        scatterpoints=1,
        labelspacing=1.5,
    )

    fig.suptitle(
        "Gene expression by cell type across CosMx slides",
        fontsize=12,
    )
    plt.savefig(OUTPUT_PATH, dpi=200, bbox_inches="tight")
    print(f"\nSaved dot plot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
