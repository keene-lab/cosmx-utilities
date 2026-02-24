#!/usr/bin/env python3
"""Process a single CosMx slide: detect segmentation, download, stitch, and upload.

Replaces process-slide-wrapper.sh with a Python implementation that uses DuckDB
to query segmentation manifests directly in S3 (zero pre-downloads) and does
targeted S3 syncs to download only the files stitch-images actually needs.

Usage:
    uv run python scripts/process-slide.py my-bucket CosMx-GBM/.../SlideName/ScanId
    uv run python scripts/process-slide.py --whatif my-bucket CosMx-GBM/.../SlideName/ScanId
    uv run python scripts/process-slide.py --segmentation-version Segmentation_uuid_003 my-bucket CosMx-GBM/.../SlideName/ScanId
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


# ── Path parsing ────────────────────────────────────────────────────────────


@dataclass
class SlideContext:
    """All path components derived from the S3 bucket + slide base path."""

    bucket: str
    slide_base_path: str  # Study/Experiment/AtoMxRun/DecodedFiles/SlideName/ScanId
    work_dir: Path = Path("/tmp/slide")

    def __post_init__(self) -> None:
        parts = self.slide_base_path.rstrip("/").split("/")
        # .../Study/Experiment/AtoMxRun/DecodedFiles/SlideName/ScanId
        self.slide_name = parts[-2]
        self.scan_id = parts[-1]
        self.atomx_run = parts[-4]
        self.experiment = parts[-5]
        self.study = parts[-6]
        self.experiment_prefix = f"{self.study}/{self.experiment}"
        self.output_prefix = (
            f"napari-stitched/{self.study}/{self.experiment}"
            f"/{self.atomx_run}/{self.slide_name}"
        )

    def s3(self, suffix: str = "") -> str:
        return f"s3://{self.bucket}/{self.slide_base_path}/{suffix}"


# ── Benchmarking ────────────────────────────────────────────────────────────


@dataclass
class Benchmark:
    """Tracks step timings and writes a benchmark CSV on success or failure."""

    ctx: SlideContext
    whatif: bool
    start_time: str = ""
    status: str = "failed"
    failed_step: str = ""
    _uploaded: bool = False
    _timings: dict[str, tuple[float | None, float | None]] = field(
        default_factory=lambda: {
            "download": (None, None),
            "stitch": (None, None),
            "read_targets": (None, None),
            "metadata": (None, None),
            "upload": (None, None),
        }
    )

    def start(self, step: str) -> float:
        self.failed_step = step
        t = time.time()
        self._timings[step] = (t, None)
        return t

    def end(self, step: str) -> float:
        t = time.time()
        start, _ = self._timings[step]
        self._timings[step] = (start, t)
        return t

    def _duration(self, step: str) -> str:
        start, end = self._timings.get(step, (None, None))
        if start is not None and end is not None:
            return str(int(end - start))
        return ""

    def _duration_seconds(self, step: str) -> int | None:
        start, end = self._timings.get(step, (None, None))
        if start is not None and end is not None:
            return int(end - start)
        return None

    def _instance_metadata(self) -> tuple[str, str]:
        vcpus = str(multiprocessing.cpu_count())
        mem_mb = ""
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        mem_mb = str(int(line.split()[1]) // 1024)
                        break
        except FileNotFoundError:
            pass
        return vcpus, mem_mb

    def _fov_stats(self) -> tuple[str, str]:
        cell_stats = self.ctx.work_dir / "CellStatsDir"
        if not cell_stats.is_dir():
            return "", ""
        fov_count = sum(1 for d in cell_stats.iterdir() if d.is_dir() and d.name.startswith("FOV"))
        total_bytes = sum(f.stat().st_size for f in cell_stats.rglob("*") if f.is_file())
        return str(fov_count), str(total_bytes)

    def write_and_upload(self) -> None:
        if self._uploaded:
            return
        self._uploaded = True

        csv_path = self.ctx.work_dir / "benchmark.csv"
        vcpus, mem_mb = self._instance_metadata()
        fov_count, fov_bytes = self._fov_stats()

        download_start, _ = self._timings.get("download", (None, None))
        _, upload_end = self._timings.get("upload", (None, None))
        total = ""
        if download_start is not None and upload_end is not None:
            total = str(int(upload_end - download_start))

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "slide_name", "study", "experiment", "atomx_run",
                "vcpus", "memory_mb", "fov_count", "fov_bytes", "start_time",
                "duration_download_s", "duration_stitch_s", "duration_read_targets_s",
                "duration_metadata_s", "duration_upload_s", "duration_total_s",
                "status", "failed_step",
            ])
            writer.writerow([
                self.ctx.slide_name, self.ctx.study, self.ctx.experiment, self.ctx.atomx_run,
                vcpus, mem_mb, fov_count, fov_bytes, self.start_time,
                self._duration("download"), self._duration("stitch"),
                self._duration("read_targets"), self._duration("metadata"),
                self._duration("upload"), total,
                self.status, self.failed_step,
            ])

        bench_name = f"benchmark_{vcpus}cpu_{mem_mb}mb.csv"
        if self.whatif:
            log(f"  Benchmark (whatif, local):")
            log(csv_path.read_text())
        else:
            s3_run(["s3", "cp", str(csv_path),
                     f"s3://{self.ctx.bucket}/{self.ctx.output_prefix}/{bench_name}"],
                    quiet=True)
            log(f"  Benchmark: s3://{self.ctx.bucket}/{self.ctx.output_prefix}/{bench_name}")


# ── Helpers ─────────────────────────────────────────────────────────────────


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    print(msg, flush=True)


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, **kwargs)


def s3_run(args: list[str], quiet: bool = False) -> subprocess.CompletedProcess:
    cmd = ["aws"] + args
    if quiet:
        cmd.append("--quiet")
    return run(cmd, capture_output=quiet)


def s3_sync(
    src: str,
    dst: str,
    *,
    exclude: str | None = None,
    includes: list[str] | None = None,
    dryrun: bool = False,
) -> None:
    cmd = ["aws", "s3", "sync", src, dst]
    if exclude:
        cmd += ["--exclude", exclude]
    for inc in (includes or []):
        cmd += ["--include", inc]
    if dryrun:
        cmd.append("--dryrun")
    run(cmd)


def s3_ls_prefixes(bucket: str, prefix: str) -> list[str]:
    """List immediate subdirectory prefixes under an S3 path."""
    result = subprocess.run(
        ["aws", "s3", "ls", f"s3://{bucket}/{prefix}/"],
        capture_output=True, text=True, check=True,
    )
    dirs = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if line.startswith("PRE "):
            dirs.append(line[4:].rstrip("/"))
    return dirs


# ── DuckDB S3 credentials ───────────────────────────────────────────────────


def _duckdb_create_s3_secret(con: "duckdb.DuckDBPyConnection") -> None:
    """Configure DuckDB S3 access using AWS CLI credentials.

    Resolves credentials the same way the AWS CLI does (env vars, SSO, profiles)
    by calling `aws configure export-credentials`. This ensures DuckDB can access
    S3 even when using SSO sessions that the credential_chain provider can't
    auto-detect.
    """
    result = subprocess.run(
        ["aws", "configure", "export-credentials", "--format", "env"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Fall back to env vars or Fargate task role (credential_chain)
        con.sql("INSTALL httpfs; LOAD httpfs;")
        return

    creds: dict[str, str] = {}
    for line in result.stdout.strip().splitlines():
        # Lines like: export AWS_ACCESS_KEY_ID=AKIA...
        line = line.replace("export ", "").strip()
        if "=" in line:
            key, _, value = line.partition("=")
            creds[key.strip()] = value.strip()

    region = (
        creds.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or os.environ.get("AWS_REGION", "us-west-2")
    )

    secret_sql = f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{creds["AWS_ACCESS_KEY_ID"]}',
            SECRET '{creds["AWS_SECRET_ACCESS_KEY"]}',
            REGION '{region}'
    """
    session_token = creds.get("AWS_SESSION_TOKEN")
    if session_token:
        secret_sql += f",\n        SESSION_TOKEN '{session_token}'"
    secret_sql += "\n    );"

    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql(secret_sql)


# ── Step 0: Detect segmentation version ────────────────────────────────────


def _read_seg_id_from_flatfiles(ctx: SlideContext) -> str:
    """Read the cellSegmentationSetId from this AtoMx run's flatFiles metadata.

    Downloads the first row of the run's metadata CSV to extract the
    segmentation UUID that was used for this specific AtoMx run's analysis.
    Returns the UUID string, or "" if not found.
    """
    import csv
    import gzip
    import tempfile

    s3_key = (
        f"{ctx.experiment_prefix}/{ctx.atomx_run}"
        f"/flatFiles/{ctx.slide_name}/{ctx.slide_name}_metadata_file.csv.gz"
    )
    s3_uri = f"s3://{ctx.bucket}/{s3_key}"

    with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ["aws", "s3", "cp", s3_uri, tmp_path, "--quiet"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            log(f"  WARNING: Could not download {s3_uri}")
            return ""

        with gzip.open(tmp_path, "rt") as f:
            reader = csv.DictReader(f)
            row = next(reader, None)
            if row:
                return row.get("cellSegmentationSetId", "").strip()
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return ""


def _find_seg_subdir_by_uuid(ctx: SlideContext, seg_id: str) -> str:
    """Find the Segmentation_* subdirectory in CellStatsDir matching a UUID.

    Queries S3 for Segmentation_* prefixes and returns the one whose name
    contains the given UUID. Returns "" if no match.
    """
    seg_dirs = s3_ls_prefixes(
        ctx.bucket,
        f"{ctx.slide_base_path}/CellStatsDir",
    )
    for dirname in seg_dirs:
        if seg_id in dirname:
            return dirname
    return ""


def _detect_highest_version(ctx: SlideContext) -> tuple[str, str]:
    """Fallback: query S3 manifest JSONs via DuckDB to find the highest version.

    Returns (celllabels_subdir, seg_uuid) or ("", "") if none found.
    """
    import duckdb

    con = duckdb.connect()
    _duckdb_create_s3_secret(con)

    s3_glob = (
        f"s3://{ctx.bucket}/{ctx.slide_base_path}"
        f"/CellStatsDir/Segmentation_*"
        f"/SegmentationManifest_Parameters_*.json"
    )
    try:
        result = con.sql(f"""
            SELECT
                regexp_extract(filename, '(Segmentation_[^/]+)', 1) AS seg_dir
            FROM read_json_auto(
                '{s3_glob}',
                filename=true,
                union_by_name=true
            )
            ORDER BY CAST(Version AS INTEGER) DESC
            LIMIT 1
        """).fetchone()
        if result and result[0]:
            subdir = result[0]
            uuid = re.sub(r"^Segmentation_", "", subdir)
            uuid = re.sub(r"_\d+$", "", uuid)
            return subdir, uuid
    except Exception as e:
        print(f"WARNING: DuckDB manifest query failed: {e}", file=sys.stderr)

    return "", ""


def detect_segmentation(ctx: SlideContext) -> tuple[str, str]:
    """Detect the correct segmentation version for this AtoMx run.

    Strategy: read cellSegmentationSetId from the AtoMx run's own flatFiles
    metadata, then find the matching Segmentation_* subdirectory in
    CellStatsDir. This ensures each run uses its own segmentation version
    when multiple resegmentations exist.

    Falls back to picking the highest version from manifest JSONs if the
    flatFiles lookup fails.

    Returns (celllabels_subdir, seg_uuid) or ("", "") if none found.
    """
    # Primary: match via flatFiles cellSegmentationSetId
    seg_id = _read_seg_id_from_flatfiles(ctx)
    if seg_id:
        log(f"  flatFiles cellSegmentationSetId: {seg_id}")
        subdir = _find_seg_subdir_by_uuid(ctx, seg_id)
        if subdir:
            return subdir, seg_id
        log(f"  WARNING: No CellStatsDir subdir matches UUID {seg_id}, "
            "falling back to highest version")

    # Fallback: highest version from manifest JSONs
    return _detect_highest_version(ctx)


# ── Step 1: Download slide data ────────────────────────────────────────────


def download_slide(ctx: SlideContext, celllabels_subdir: str, dryrun: bool = False) -> str:
    """Download only the files needed for processing. Returns the AnalysisResults subdir name."""
    # CellStatsDir: CellLabels TIFs + Morphology2D images only
    s3_sync(
        ctx.s3("CellStatsDir/"), str(ctx.work_dir / "CellStatsDir/"),
        exclude="*",
        includes=[
            "FOV*/CellLabels_F*.tif", "FOV*/CellLabels_F*.TIF",
            "Morphology2D/*.TIF", "Morphology2D/*.tif",
        ],
        dryrun=dryrun,
    )

    # Selected segmentation version's CellLabels only
    if celllabels_subdir:
        s3_sync(
            ctx.s3("CellStatsDir/"), str(ctx.work_dir / "CellStatsDir/"),
            exclude="*",
            includes=[
                f"{celllabels_subdir}/FOV*/CellLabels_F*.tif",
                f"{celllabels_subdir}/FOV*/CellLabels_F*.TIF",
            ],
            dryrun=dryrun,
        )

    # RunSummary: only FOV location CSVs
    s3_sync(
        ctx.s3("RunSummary/"), str(ctx.work_dir / "RunSummary/"),
        exclude="*",
        includes=["*.fovs.csv", "*FOV_Locations*"],
        dryrun=dryrun,
    )

    # AnalysisResults: full sync (needed for read-targets)
    subdirs = s3_ls_prefixes(ctx.bucket, f"{ctx.slide_base_path}/AnalysisResults")
    if len(subdirs) != 1:
        raise RuntimeError(
            f"Expected exactly 1 AnalysisResults subdirectory, found {len(subdirs)}: {subdirs}"
        )
    analysis_subdir = subdirs[0]
    s3_sync(
        ctx.s3(f"AnalysisResults/{analysis_subdir}/"),
        str(ctx.work_dir / "AnalysisResults" / analysis_subdir / ""),
        dryrun=dryrun,
    )

    return analysis_subdir


# ── Step 2: Stitch images ──────────────────────────────────────────────────


def stitch_images(ctx: SlideContext, celllabels_subdir: str) -> None:
    cmd = [
        "uv", "run", "stitch-images",
        "-i", str(ctx.work_dir / "CellStatsDir"),
        "-f", str(ctx.work_dir / "RunSummary"),
        "-o", str(ctx.work_dir / "output"),
    ]
    if celllabels_subdir:
        cmd += ["--celllabels-subdir", celllabels_subdir]
    run(cmd)


# ── Step 3: Read targets ───────────────────────────────────────────────────


def read_targets(ctx: SlideContext, analysis_subdir: str) -> None:
    run([
        "uv", "run", "read-targets",
        str(ctx.work_dir / "AnalysisResults" / analysis_subdir),
        "-o", str(ctx.work_dir / "output"),
        "--filename", "targets.hdf5",
    ])


# ── Step 4: Generate metadata CSV ──────────────────────────────────────────


def generate_metadata(ctx: SlideContext, seg_uuid: str) -> None:
    script_dir = Path(__file__).resolve().parent
    metadata_script = script_dir / "generate-slide-metadata.py"
    if not metadata_script.exists():
        metadata_script = Path("/app/scripts/generate-slide-metadata.py")

    cmd = [
        "uv", "run", "python", str(metadata_script),
        "--bucket", ctx.bucket,
        "--experiment-prefix", ctx.experiment_prefix,
        "--slide-name", ctx.slide_name,
        "--output", str(ctx.work_dir / "output" / "_metadata.csv"),
    ]
    if seg_uuid:
        cmd += ["--seg-id", seg_uuid]
    run(cmd)


# ── Step 5: Upload results ─────────────────────────────────────────────────


def upload_results(ctx: SlideContext) -> None:
    s3_sync(
        str(ctx.work_dir / "output/"),
        f"s3://{ctx.bucket}/{ctx.output_prefix}/",
    )


# ── Main ────────────────────────────────────────────────────────────────────


def process_slide(
    ctx: SlideContext,
    *,
    whatif: bool = False,
    seg_version_override: str = "",
) -> None:
    bench = Benchmark(ctx=ctx, whatif=whatif)

    log("=== Process Slide ===")
    if whatif:
        log("  Mode:       whatif (dry run)")
    log(f"  Study:      {ctx.study}")
    log(f"  Experiment: {ctx.experiment}")
    log(f"  AtoMx run:  {ctx.atomx_run}")
    log(f"  Slide:      {ctx.slide_name}")
    log(f"  Output:     s3://{ctx.bucket}/{ctx.output_prefix}/")
    log("")

    bench.start_time = now_iso()

    # Clean up any previous run
    if ctx.work_dir.exists():
        shutil.rmtree(ctx.work_dir)
    ctx.work_dir.mkdir(parents=True)

    try:
        if seg_version_override:
            celllabels_subdir = seg_version_override
            seg_uuid = re.sub(r"^Segmentation_", "", celllabels_subdir)
            seg_uuid = re.sub(r"_\d+$", "", seg_uuid)
            log(f"  Using segmentation override: {celllabels_subdir}")
        else:
            bench.failed_step = "detect_segmentation"
            log(f"[{now_iso()}] Detecting segmentation version (DuckDB -> S3) ...")
            celllabels_subdir, seg_uuid = detect_segmentation(ctx)
            if celllabels_subdir:
                log(f"  Matched segmentation: {celllabels_subdir}")
                log(f"  Segmentation UUID:    {seg_uuid}")
            else:
                log("  No segmentation manifests found, using base CellStatsDir")

        bench.start("download")
        log(f"[{now_iso()}] Downloading slide data ...")
        analysis_subdir = download_slide(ctx, celllabels_subdir, dryrun=whatif)
        bench.end("download")
        dur = bench._duration_seconds("download")
        log(f"[{now_iso()}] Download complete ({dur}s)")

        if whatif:
            # In whatif mode, we've shown what would be synced — skip processing steps
            for step in ("stitch", "read_targets", "metadata", "upload"):
                bench.start(step)
                log(f"[{now_iso()}] [whatif] {step} ... skipped")
                bench.end(step)
        else:
            bench.start("stitch")
            log(f"[{now_iso()}] Stitching images ...")
            stitch_images(ctx, celllabels_subdir)
            bench.end("stitch")
            log(f"[{now_iso()}] Stitch complete ({bench._duration_seconds('stitch')}s)")

            bench.start("read_targets")
            log(f"[{now_iso()}] Reading targets ...")
            read_targets(ctx, analysis_subdir)
            bench.end("read_targets")
            log(f"[{now_iso()}] Read targets complete ({bench._duration_seconds('read_targets')}s)")

            bench.start("metadata")
            log(f"[{now_iso()}] Generating metadata CSV ...")
            generate_metadata(ctx, seg_uuid)
            bench.end("metadata")
            log(f"[{now_iso()}] Metadata complete ({bench._duration_seconds('metadata')}s)")

            bench.start("upload")
            log(f"[{now_iso()}] Uploading results ...")
            upload_results(ctx)
            bench.end("upload")
            log(f"[{now_iso()}] Upload complete ({bench._duration_seconds('upload')}s)")

        bench.status = "success"
        bench.failed_step = ""

    except Exception:
        log(f"\n=== FAILED at step: {bench.failed_step or 'unknown'} ===")
        bench.write_and_upload()
        raise

    bench.write_and_upload()

    download_start, _ = bench._timings["download"]
    _, upload_end = bench._timings["upload"]
    if download_start and upload_end:
        log(f"\n=== Done (total {int(upload_end - download_start)}s) ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process a single CosMx slide: detect segmentation, download, stitch, and upload.",
    )
    parser.add_argument("s3_bucket", help="S3 bucket name")
    parser.add_argument(
        "slide_base_path",
        help="Path within bucket to slide (Study/Experiment/AtoMxRun/DecodedFiles/SlideName/ScanId)",
    )
    parser.add_argument("--whatif", action="store_true", help="Dry run: detect segmentation and show what would be synced")
    parser.add_argument(
        "--segmentation-version",
        default="",
        help="Override segmentation version subdirectory (e.g. Segmentation_uuid_003)",
    )
    args = parser.parse_args()

    ctx = SlideContext(bucket=args.s3_bucket, slide_base_path=args.slide_base_path)
    process_slide(ctx, whatif=args.whatif, seg_version_override=args.segmentation_version)


if __name__ == "__main__":
    main()
