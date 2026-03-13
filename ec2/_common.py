"""Shared utilities for EC2 analytics scripts."""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

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


def aws(*args: str, parse_json: bool = False) -> subprocess.CompletedProcess | dict:
    """Run an AWS CLI command. If parse_json=True, return parsed JSON output."""
    cmd = ["aws"] + list(args)
    if parse_json:
        cmd += ["--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"AWS CLI error: {' '.join(cmd)}", file=sys.stderr)
        print(result.stderr, file=sys.stderr)
        sys.exit(1)
    if parse_json:
        return json.loads(result.stdout)
    return result
