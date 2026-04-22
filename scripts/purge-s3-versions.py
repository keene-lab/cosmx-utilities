#!/usr/bin/env python3
"""Permanently purge all object versions and delete markers under an S3 prefix.

Intended companion to the keene-cosmx-data-DELETER role on the versioned
keene-cosmx-data bucket. A normal ``aws s3 rm`` on a versioned bucket only
places delete markers — the underlying object bytes and old versions remain
and continue to incur storage. Running this script (assumed into DELETER)
purges them permanently.

Usage:
    AWS_PROFILE=delete-cosmx uv run python scripts/purge-s3-versions.py \\
        --bucket keene-cosmx-data --prefix path/to/clean/

Flags:
    --dry-run   List what would be purged, then exit without deleting.
    --yes       Skip the interactive confirmation prompt.
"""

import argparse
import sys
from typing import Iterator

import boto3

# S3 DeleteObjects API accepts at most 1000 keys per call.
BATCH_SIZE = 1000


def log(msg: str) -> None:
    print(msg, flush=True)


def iter_all_versions(s3, bucket: str, prefix: str) -> Iterator[dict]:
    """Yield {'Key': ..., 'VersionId': ...} for every version and delete marker."""
    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Versions") or []:
            yield {"Key": entry["Key"], "VersionId": entry["VersionId"]}
        for entry in page.get("DeleteMarkers") or []:
            yield {"Key": entry["Key"], "VersionId": entry["VersionId"]}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Permanently purge S3 object versions and delete markers under a prefix."
    )
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True, help="S3 key prefix to purge")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List entries that would be purged, then exit.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the interactive confirmation prompt.",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3")

    log(f"Listing all versions under s3://{args.bucket}/{args.prefix} ...")
    entries = list(iter_all_versions(s3, args.bucket, args.prefix))
    log(f"Found {len(entries)} versions + delete markers to purge.")

    if not entries:
        return 0

    if args.dry_run:
        for e in entries[:10]:
            log(f"  {e['Key']} (VersionId={e['VersionId']})")
        if len(entries) > 10:
            log(f"  ... and {len(entries) - 10} more")
        log("Dry run — nothing deleted.")
        return 0

    if not args.yes:
        confirm = input(f"Permanently delete these {len(entries)} entries? [y/N] ")
        if confirm.strip().lower() != "y":
            log("Aborted.")
            return 1

    deleted = 0
    for i in range(0, len(entries), BATCH_SIZE):
        batch = entries[i : i + BATCH_SIZE]
        resp = s3.delete_objects(
            Bucket=args.bucket,
            Delete={"Objects": batch, "Quiet": True},
        )
        errors = resp.get("Errors") or []
        if errors:
            log(f"  Batch had {len(errors)} errors; first: {errors[0]}")
            return 1
        deleted += len(batch)
        log(f"  Purged {deleted}/{len(entries)}")

    log(f"Done. Permanently purged {deleted} versions + delete markers.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
