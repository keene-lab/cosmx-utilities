#!/usr/bin/env python3
"""Generate _metadata.csv for a single slide, matching the segmentation version
used for stitching.

When a slide has been resegmented, multiple AtoMx runs may exist under the same
experiment, each with its own flatFiles metadata tied to a specific segmentation.
This script finds the correct metadata source by matching the
cellSegmentationSetId UUID and produces a _metadata.csv whose cell IDs align
with the CellLabels TIFFs selected by the stitcher.

Usage (called from process-slide.py):
    uv run python scripts/generate-slide-metadata.py \
        --bucket my-bucket \
        --experiment-prefix CosMx-GBM/CosMx-GBM-segmentation-test-1.9.26 \
        --slide-name UWA7522G2G5Glioblastoma \
        --seg-id 12d18c13-3b25-4cbf-be1a-24d6c24703d5 \
        --output /tmp/slide/output/_metadata.csv

    When --seg-id is omitted, all cells from the first metadata file found are
    used (backwards-compatible with single-segmentation slides).
"""

import argparse
import csv
import gzip
import os
import sys
import tempfile

import boto3
import duckdb
from botocore.exceptions import ClientError


def s3_ls_prefixes(s3, bucket: str, prefix: str) -> list[str]:
    """List immediate subdirectory prefixes under an S3 path."""
    response = s3.list_objects_v2(
        Bucket=bucket, Prefix=prefix.rstrip("/") + "/", Delimiter="/",
    )
    return [
        p["Prefix"].rstrip("/").rsplit("/", 1)[-1]
        for p in response.get("CommonPrefixes", [])
    ]


def s3_file_exists(s3, bucket: str, key: str) -> bool:
    """Check if an S3 object exists."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def s3_download(s3, bucket: str, key: str, local_path: str) -> bool:
    """Download an S3 object to a local path. Returns True on success."""
    try:
        s3.download_file(bucket, key, local_path)
        return True
    except ClientError:
        return False


def deterministic_color(value: str) -> str:
    """Generate a deterministic hex color from a string using DuckDB's hash()."""
    result = duckdb.sql(
        f"SELECT printf('#%06X', abs(hash($1)) % 16777216)",
        params=[value],
    ).fetchone()
    return result[0]


def find_metadata_file(
    s3, bucket: str, experiment_prefix: str, slide_name: str
) -> list[tuple[str, str]]:
    """Find all metadata files for a slide across all AtoMx runs.

    Returns list of (atomx_run_name, s3_key) tuples.
    """
    atomx_runs = s3_ls_prefixes(s3, bucket, experiment_prefix)
    results = []
    for run in atomx_runs:
        key = f"{experiment_prefix}/{run}/flatFiles/{slide_name}/{slide_name}_metadata_file.csv.gz"
        if s3_file_exists(s3, bucket, key):
            results.append((run, key))
    return results


def generate_metadata(
    input_path: str,
    output_path: str,
    seg_id: str | None,
    cell_type_column: str,
) -> dict:
    """Read a gzipped metadata CSV, filter by segmentation ID, and write
    _metadata.csv.

    Returns a dict with statistics.
    """
    # Read and filter
    rows = []
    total_read = 0
    seg_ids_seen = set()
    # Support multiple comma-separated UUIDs
    seg_id_set = set(s.strip() for s in seg_id.split(",")) if seg_id else None

    with gzip.open(input_path, "rt") as f:
        reader = csv.DictReader(f)

        for row in reader:
            total_read += 1
            row_seg_id = row.get("cellSegmentationSetId", "").strip()
            seg_ids_seen.add(row_seg_id)

            if seg_id_set is not None and row_seg_id not in seg_id_set:
                continue

            cell_id = row.get("cell_id", "")
            cell_type = row.get(cell_type_column, "")
            rows.append((cell_id, cell_type))

    # Build color map (deterministic per cell type)
    cell_types = sorted(set(ct for _, ct in rows if ct))
    color_map = {ct: deterministic_color(ct) for ct in cell_types}

    # Write output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["cell_ID", "cell_type", "hex_color"])
        for cell_id, cell_type in rows:
            hex_color = color_map.get(cell_type, "")
            writer.writerow([cell_id, cell_type, hex_color])

    return {
        "total_read": total_read,
        "total_written": len(rows),
        "total_filtered": total_read - len(rows),
        "cell_types": len(cell_types),
        "seg_ids_seen": seg_ids_seen,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate _metadata.csv for a slide, matching the segmentation version.",
    )
    parser.add_argument(
        "--bucket", required=True,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--experiment-prefix", required=True,
        help="S3 prefix for the experiment (e.g. CosMx-GBM/CosMx-GBM-segmentation-test-1.9.26)",
    )
    parser.add_argument(
        "--slide-name", required=True,
        help="Slide name (e.g. UWA7522G2G5Glioblastoma)",
    )
    parser.add_argument(
        "--seg-id", default=None,
        help="cellSegmentationSetId UUID to filter by (extracted from CellLabels subdir name). "
             "When omitted, all cells are included.",
    )
    parser.add_argument(
        "--cell-type-column",
        default="RNA_RNA_Cell.Typing.InSituType.Core.GBmap_1_clusters",
        help="Column name for cell type annotations",
    )
    parser.add_argument(
        "--output", required=True,
        help="Output path for _metadata.csv",
    )
    args = parser.parse_args()

    print(f"  Slide:     {args.slide_name}")
    print(f"  Seg ID:    {args.seg_id or '(all)'}")
    print(f"  Cell type: {args.cell_type_column}")

    s3 = boto3.client("s3")

    # Find all metadata files for this slide
    sources = find_metadata_file(
        s3, args.bucket, args.experiment_prefix, args.slide_name,
    )

    if not sources:
        print(f"ERROR: No metadata files found for slide {args.slide_name}", file=sys.stderr)
        print(f"  Searched: s3://{args.bucket}/{args.experiment_prefix}/*/flatFiles/{args.slide_name}/", file=sys.stderr)
        sys.exit(1)

    print(f"  Found {len(sources)} metadata source(s):")
    for run_name, key in sources:
        print(f"    - {run_name}")

    # If we have a seg-id, try each source until we find one with matching cells.
    # If no seg-id, just use the first source.
    with tempfile.TemporaryDirectory() as tmpdir:
        local_gz = os.path.join(tmpdir, "metadata.csv.gz")

        if args.seg_id is None:
            # No filtering — use first source
            run_name, s3_key = sources[0]
            print(f"  Downloading from: {run_name}")
            if not s3_download(s3, args.bucket, s3_key, local_gz):
                print(f"ERROR: Failed to download s3://{args.bucket}/{s3_key}", file=sys.stderr)
                sys.exit(1)

            stats = generate_metadata(
                local_gz, args.output, None, args.cell_type_column,
            )
            print(f"  Generated: {stats['total_written']} cells, {stats['cell_types']} types")
            return

        # Multiple seg IDs may span multiple AtoMx runs (e.g. two-step
        # resegmentation). Collect matching rows from ALL sources, then
        # write a single merged _metadata.csv.
        seg_ids_requested = set(s.strip() for s in args.seg_id.split(","))
        seg_ids_found = set()
        all_rows = []
        all_seg_ids_seen = set()

        for run_name, s3_key in sources:
            print(f"  Trying: {run_name} ...")
            if not s3_download(s3, args.bucket, s3_key, local_gz):
                print(f"    Download failed, skipping")
                continue

            stats = generate_metadata(
                local_gz, args.output, args.seg_id, args.cell_type_column,
            )
            all_seg_ids_seen.update(stats["seg_ids_seen"])

            if stats["total_written"] > 0:
                # Read back the rows we just wrote (generate_metadata wrote to output)
                with open(args.output) as f:
                    reader = csv.DictReader(f)
                    new_rows = [(r["cell_ID"], r["cell_type"], r.get("hex_color", ""))
                                for r in reader]
                all_rows += new_rows
                matched_ids = seg_ids_requested & stats["seg_ids_seen"]
                seg_ids_found.update(matched_ids)
                print(f"    Matched {stats['total_written']} cells "
                      f"(seg IDs: {matched_ids})")

                # If we've found all requested seg IDs, stop early
                if seg_ids_found >= seg_ids_requested:
                    break
            else:
                print(f"    No matching cells (seg IDs in file: {stats['seg_ids_seen']})")

        if not all_rows:
            print(f"ERROR: No metadata source contains cells for "
                  f"segmentation IDs {seg_ids_requested}", file=sys.stderr)
            print(f"  Seg IDs found across all sources: {all_seg_ids_seen}",
                  file=sys.stderr)
            sys.exit(1)

        # Write merged output
        # Rebuild color map across all collected cell types
        cell_types = sorted(set(ct for _, ct, _ in all_rows if ct))
        color_map = {ct: deterministic_color(ct) for ct in cell_types}

        os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
        with open(args.output, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["cell_ID", "cell_type", "hex_color"])
            for cell_id, cell_type, _ in all_rows:
                hex_color = color_map.get(cell_type, "")
                writer.writerow([cell_id, cell_type, hex_color])

        print(f"  Merged metadata: {len(all_rows)} cells, {len(cell_types)} types "
              f"from {len(seg_ids_found)} segmentation(s)")


if __name__ == "__main__":
    main()
