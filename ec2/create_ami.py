#!/usr/bin/env python3
"""Build a custom AMI for CosMx analytics EC2 instances.

Launches a temporary builder instance with ami_setup.sh as user-data,
waits for setup to complete, creates an AMI snapshot, and cleans up.

Usage:
    uv run python ec2/create_ami.py
    uv run python ec2/create_ami.py --name my-custom-ami
    uv run python ec2/create_ami.py --keep-builder   # don't terminate on completion
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

from _common import (
    AMI_SETUP_SCRIPT,
    ENV_PATH,
    PROJECT_TAG,
    ROOT_VOLUME_GB,
    aws,
    env,
    log,
)

BUILDER_INSTANCE_TYPE = "t3.medium"
POLL_INTERVAL_SECONDS = 60
SETUP_TIMEOUT_SECONDS = 45 * 60  # 45 minutes


def launch_builder(region: str, ami: str, subnet: str, sg: str,
                   key_pair: str, profile: str) -> str:
    """Launch the temporary builder instance. Returns instance ID."""
    block_devices = json.dumps([{
        "DeviceName": "/dev/sda1",
        "Ebs": {"VolumeSize": ROOT_VOLUME_GB, "VolumeType": "gp3"},
    }])
    tags = json.dumps([
        {
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": "cosmx-ami-builder"},
                {"Key": "Project", "Value": PROJECT_TAG},
            ],
        },
        {
            "ResourceType": "volume",
            "Tags": [
                {"Key": "Name", "Value": "cosmx-ami-builder"},
                {"Key": "Project", "Value": PROJECT_TAG},
            ],
        },
    ])

    result = aws(
        "ec2", "run-instances",
        "--region", region,
        "--image-id", ami,
        "--instance-type", BUILDER_INSTANCE_TYPE,
        "--key-name", key_pair,
        "--subnet-id", subnet,
        "--security-group-ids", sg,
        "--iam-instance-profile", f"Name={profile}",
        "--user-data", f"file://{AMI_SETUP_SCRIPT}",
        "--block-device-mappings", block_devices,
        "--tag-specifications", tags,
        parse_json=True,
    )
    return result["Instances"][0]["InstanceId"]


def poll_setup_completion(region: str, instance_id: str) -> bool:
    """Poll via SSM for the sentinel file indicating setup is complete."""
    log("Waiting for AMI setup to complete (this takes 15-30 minutes)...")
    start = time.monotonic()

    while time.monotonic() - start < SETUP_TIMEOUT_SECONDS:
        elapsed = int(time.monotonic() - start)
        minutes = elapsed // 60
        print(f"\r  [{minutes}m elapsed] Checking setup status...", end="", flush=True)

        try:
            send_result = aws(
                "ssm", "send-command",
                "--region", region,
                "--instance-ids", instance_id,
                "--document-name", "AWS-RunShellScript",
                "--parameters",
                json.dumps({"commands": [
                    "test -f /var/lib/cloud/instance/ami-setup-complete && echo READY || echo PENDING"
                ]}),
                "--output-s3-bucket-name", "",
                parse_json=True,
            )
            command_id = send_result["Command"]["CommandId"]
        except (SystemExit, KeyError):
            # SSM agent may not be ready yet — wait and retry
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        # Wait a moment for the command to execute
        time.sleep(10)

        try:
            invocation = aws(
                "ssm", "get-command-invocation",
                "--region", region,
                "--command-id", command_id,
                "--instance-id", instance_id,
                parse_json=True,
            )
            output = invocation.get("StandardOutputContent", "").strip()
            if output == "READY":
                print()  # newline after progress
                return True
        except SystemExit:
            pass  # command not yet complete

        time.sleep(POLL_INTERVAL_SECONDS)

    print()
    return False


def create_image(region: str, instance_id: str, ami_name: str) -> str:
    """Stop the instance and create an AMI from it. Returns AMI ID."""
    log("Stopping builder instance for clean snapshot...")
    aws("ec2", "stop-instances", "--region", region, "--instance-ids", instance_id)
    aws("ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", instance_id)

    log(f"Creating AMI: {ami_name}")
    result = aws(
        "ec2", "create-image",
        "--region", region,
        "--instance-id", instance_id,
        "--name", ami_name,
        "--description", "CosMx analytics: R + RStudio + UV + Python + Jupyter + DCV",
        parse_json=True,
    )
    ami_id = result["ImageId"]

    log(f"Waiting for AMI {ami_id} to become available...")
    aws("ec2", "wait", "image-available", "--region", region, "--image-ids", ami_id)
    return ami_id


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a CosMx analytics AMI")
    parser.add_argument(
        "--name",
        help="Custom AMI name (default: cosmx-analytics-YYYYMMDD-HHMMSS)",
    )
    parser.add_argument(
        "--keep-builder",
        action="store_true",
        help="Don't terminate the builder instance (useful for debugging)",
    )
    args = parser.parse_args()

    if not AMI_SETUP_SCRIPT.exists():
        print(f"ERROR: {AMI_SETUP_SCRIPT} not found", file=sys.stderr)
        sys.exit(1)

    load_dotenv(ENV_PATH)
    region = env("AWS_REGION")
    base_ami = env("UBUNTU_BASE_AMI")
    subnet = env("EC2_SUBNET")
    sg = env("EC2_SECURITY_GROUP")
    key_pair = env("EC2_KEY_PAIR")
    profile = env("EC2_INSTANCE_PROFILE")

    ami_name = args.name or f"cosmx-analytics-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"

    log("Building CosMx analytics AMI")
    log(f"  Base AMI:       {base_ami}")
    log(f"  Builder type:   {BUILDER_INSTANCE_TYPE}")
    log(f"  AMI name:       {ami_name}")

    # Launch builder
    instance_id = launch_builder(region, base_ami, subnet, sg, key_pair, profile)
    log(f"Builder instance launched: {instance_id}")

    try:
        log("Waiting for instance to reach 'running' state...")
        aws("ec2", "wait", "instance-running",
            "--region", region, "--instance-ids", instance_id)
        log("Builder instance is running")

        # Poll for setup completion
        if not poll_setup_completion(region, instance_id):
            log("ERROR: AMI setup timed out.")
            log(f"SSH into the builder to debug: aws ssm start-session --target {instance_id}")
            log("Check /var/log/ami-setup.log for details")
            if not args.keep_builder:
                log(f"Builder instance {instance_id} left running for debugging")
            sys.exit(1)

        log("AMI setup completed successfully")

        # Create AMI
        ami_id = create_image(region, instance_id, ami_name)
        log(f"AMI created: {ami_id}")

        # Cleanup
        if args.keep_builder:
            log(f"Builder instance kept: {instance_id}")
        else:
            log("Terminating builder instance...")
            aws("ec2", "terminate-instances", "--region", region, "--instance-ids", instance_id)
            log("Builder instance terminated")

        # Print result
        print()
        print(f"  AMI ID: {ami_id}")
        print()
        print("Add this to ec2/.env:")
        print(f"  EC2_AMI_ID={ami_id}")

    except KeyboardInterrupt:
        print()
        log(f"Interrupted. Builder instance {instance_id} is still running.")
        log(f"To terminate: aws ec2 terminate-instances --instance-ids {instance_id}")
        sys.exit(1)


if __name__ == "__main__":
    main()
