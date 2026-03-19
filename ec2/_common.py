"""Shared utilities for EC2 analytics scripts."""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

EC2_DIR = Path(__file__).resolve().parent
ENV_PATH = EC2_DIR / ".env"
AMI_SETUP_SCRIPT = EC2_DIR / "ami_setup.sh"
ROOT_VOLUME_GB = 64
PROJECT_TAG = "cosmx-analytics"


def env(key: str) -> str:
    """Get a required environment variable."""
    value = os.environ.get(key)
    if not value:
        print(f"ERROR: Missing required env var {key}. Check ec2/.env", file=sys.stderr)
        sys.exit(1)
    return value


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def boto_session(region: str) -> boto3.Session:
    """Create a boto3 session using the current AWS CLI credentials."""
    return boto3.Session(region_name=region)
