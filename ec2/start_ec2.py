#!/usr/bin/env python3
"""Launch a CosMx analytics EC2 instance from a custom AMI.

Spins up an r5a.4xlarge (or custom type) with a configurable secondary
EBS data volume that is auto-formatted and mounted at /mnt/cosmx.

Usage:
    uv run python ec2/start_ec2.py --name emily-gbm-analysis
    uv run python ec2/start_ec2.py --name emily-gbm-analysis --storage 1024
    uv run python ec2/start_ec2.py --name emily-gbm-analysis --instance-type r5a.8xlarge

    # Test ami_setup.sh on a raw Ubuntu instance (no AMI needed):
    uv run python ec2/start_ec2.py --name test-ami-setup --raw
"""

import argparse
import base64
import json
import sys

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

DEFAULT_INSTANCE_TYPE = "r5a.4xlarge"
DEFAULT_STORAGE_GB = 512

# Bash snippet to find, format, and mount the secondary NVMe data volume.
# On Nitro instances (r5a), /dev/sdf appears as /dev/nvme1n1.
MOUNT_VOLUME_SNIPPET = """\
# ── Mount secondary data volume ──────────────────────────────────────────
DEVICE=""
for d in /dev/nvme*n1; do
    ROOT_DEV=$(findmnt -n -o SOURCE /)
    ROOT_NVME=$(readlink -f "$ROOT_DEV" | sed 's/p[0-9]*$//')
    if [ "$d" != "$ROOT_NVME" ]; then
        DEVICE="$d"
        break
    fi
done

if [ -z "$DEVICE" ]; then
    echo "No secondary NVMe device found" >> /var/log/mount-data.log
else
    if ! blkid "$DEVICE" | grep -q TYPE; then
        mkfs.ext4 "$DEVICE"
    fi
    mkdir -p /mnt/cosmx
    mount "$DEVICE" /mnt/cosmx
    chown ubuntu:ubuntu /mnt/cosmx
    UUID=$(blkid -s UUID -o value "$DEVICE")
    if ! grep -q "$UUID" /etc/fstab; then
        echo "UUID=$UUID /mnt/cosmx ext4 defaults,nofail 0 2" >> /etc/fstab
    fi
    echo "Data volume mounted at /mnt/cosmx" >> /var/log/mount-data.log
fi
"""

MOUNT_USER_DATA = f"""\
#!/bin/bash
set -euo pipefail
{MOUNT_VOLUME_SNIPPET}"""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch a CosMx analytics EC2 instance",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Instance name tag (e.g. emily-gbm-analysis)",
    )
    parser.add_argument(
        "--storage",
        type=int,
        default=DEFAULT_STORAGE_GB,
        help=f"Secondary EBS data volume size in GB (default: {DEFAULT_STORAGE_GB})",
    )
    parser.add_argument(
        "--instance-type",
        default=DEFAULT_INSTANCE_TYPE,
        help=f"EC2 instance type (default: {DEFAULT_INSTANCE_TYPE})",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Launch a raw Ubuntu instance with ami_setup.sh as user-data "
             "(for testing setup without building an AMI)",
    )
    args = parser.parse_args()

    load_dotenv(ENV_PATH)
    region = env("AWS_REGION")
    subnet = env("EC2_SUBNET")
    sg = env("EC2_SECURITY_GROUP")
    key_pair = env("EC2_KEY_PAIR")
    profile = env("EC2_INSTANCE_PROFILE")

    if args.raw:
        ami_id = env("UBUNTU_BASE_AMI")
        if not AMI_SETUP_SCRIPT.exists():
            print(f"ERROR: {AMI_SETUP_SCRIPT} not found", file=sys.stderr)
            sys.exit(1)
        setup_script = AMI_SETUP_SCRIPT.read_text()
        user_data_str = setup_script.rstrip() + "\n\n" + MOUNT_VOLUME_SNIPPET
    else:
        ami_id = env("EC2_AMI_ID")
        user_data_str = MOUNT_USER_DATA

    mode = "RAW (ami_setup.sh)" if args.raw else "AMI"
    log(f"Launching CosMx analytics instance [{mode}]")
    log(f"  AMI:            {ami_id}")
    log(f"  Instance type:  {args.instance_type}")
    log(f"  Data volume:    {args.storage} GB")
    log(f"  Name:           {args.name}")

    block_devices = json.dumps([
        {
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": ROOT_VOLUME_GB, "VolumeType": "gp3"},
        },
        {
            "DeviceName": "/dev/sdf",
            "Ebs": {
                "VolumeSize": args.storage,
                "VolumeType": "gp3",
                "DeleteOnTermination": True,
            },
        },
    ])

    resource_tags = [
        {"Key": "Name", "Value": args.name},
        {"Key": "Project", "Value": PROJECT_TAG},
    ]
    tags = json.dumps([
        {"ResourceType": "instance", "Tags": resource_tags},
        {"ResourceType": "volume", "Tags": resource_tags},
    ])

    user_data_b64 = base64.b64encode(user_data_str.encode()).decode()

    result = aws(
        "ec2", "run-instances",
        "--region", region,
        "--image-id", ami_id,
        "--instance-type", args.instance_type,
        "--key-name", key_pair,
        "--subnet-id", subnet,
        "--security-group-ids", sg,
        "--iam-instance-profile", f"Name={profile}",
        "--user-data", user_data_b64,
        "--block-device-mappings", block_devices,
        "--tag-specifications", tags,
        parse_json=True,
    )

    instance_id = result["Instances"][0]["InstanceId"]
    log(f"Instance launched: {instance_id}")

    log("Waiting for instance to start...")
    aws("ec2", "wait", "instance-running",
        "--region", region, "--instance-ids", instance_id)

    # Get private IP
    desc = aws(
        "ec2", "describe-instances",
        "--region", region,
        "--instance-ids", instance_id,
        "--query", "Reservations[0].Instances[0].PrivateIpAddress",
        "--output", "text",
    )
    private_ip = desc.stdout.strip()

    log("Instance is running")
    print()
    print(f"  Instance ID:  {instance_id}")
    print(f"  Private IP:   {private_ip}")
    print(f"  Data volume:  {args.storage} GB (auto-mounted at /mnt/cosmx)")
    print()

    if args.raw:
        print("NOTE: --raw mode — ami_setup.sh is running as user-data.")
        print("  Setup takes ~15-30 minutes. Monitor progress:")
        print(f"  aws ssm start-session --target {instance_id} --region {region}")
        print("  Then: tail -f /var/log/ami-setup.log")
        print()

    print("Connect via SSM:")
    print(f"  aws ssm start-session --target {instance_id} --region {region}")
    print()
    print("Connect via DCV (remote desktop on port 8443):")
    print(f"  Set a password first: aws ssm start-session --target {instance_id}")
    print("  Then run: sudo passwd ubuntu")
    print(f"  Open DCV client and connect to {private_ip}")
    print()
    print("Sync data from S3:")
    print(f"  ./scripts/sync-to-ec2.sh s3://your-bucket/napari-stitched/study/experiment/ {instance_id}")
    print()
    print("Launch Jupyter (on the instance):")
    print("  cd /opt/cosmx-utilities/ec2/analytics && uv run jupyter lab --ip 0.0.0.0 --port 8888")
    print()
    print("Launch Napari (on the instance via DCV):")
    print("  cd /opt/cosmx-utilities && uv run napari /mnt/cosmx")
    print()
    print("When done, stop or terminate the instance:")
    print(f"  aws ec2 stop-instances --instance-ids {instance_id} --region {region}")
    print(f"  aws ec2 terminate-instances --instance-ids {instance_id} --region {region}")
    print()
    print("Find all cosmx-analytics resources by tag:")
    print(f"  aws ec2 describe-instances --region {region} --filters Name=tag:Project,Values={PROJECT_TAG} Name=instance-state-name,Values=running,stopped --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`Name`].Value|[0],State.Name]' --output table")


if __name__ == "__main__":
    main()
