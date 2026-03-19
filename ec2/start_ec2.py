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
import os
import subprocess
import sys

from botocore.exceptions import ClientError, WaiterError
from dotenv import load_dotenv

from _common import (
    AMI_SETUP_SCRIPT,
    ENV_PATH,
    PROJECT_TAG,
    ROOT_VOLUME_GB,
    boto_session,
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

    dcv_password = os.environ.get("DCV_PASSWORD")

    # Detect current git branch so the instance clones the right code
    try:
        git_branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            text=True,
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        git_branch = "main"

    if args.raw:
        ami_id = env("UBUNTU_BASE_AMI")
        if not AMI_SETUP_SCRIPT.exists():
            print(f"ERROR: {AMI_SETUP_SCRIPT} not found", file=sys.stderr)
            sys.exit(1)
        setup_script = AMI_SETUP_SCRIPT.read_text()
        user_data = setup_script.rstrip() + "\n\n" + MOUNT_VOLUME_SNIPPET
    else:
        ami_id = env("EC2_AMI_ID")
        user_data = MOUNT_USER_DATA

    # Inject variables after the set -e line in user-data
    injected = f'export GIT_BRANCH="{git_branch}"\n'
    if dcv_password:
        injected += f'echo "ubuntu:{dcv_password}" | chpasswd\n'
    for marker in ("set -euxo pipefail\n", "set -euo pipefail\n"):
        if marker in user_data:
            user_data = user_data.replace(marker, marker + injected, 1)
            break

    session = boto_session(region)
    ec2 = session.client("ec2")

    mode = "RAW (ami_setup.sh)" if args.raw else "AMI"
    log(f"Launching CosMx analytics instance [{mode}]")
    log(f"  AMI:            {ami_id}")
    log(f"  Instance type:  {args.instance_type}")
    log(f"  Data volume:    {args.storage} GB")
    log(f"  Git branch:     {git_branch}")
    log(f"  Name:           {args.name}")

    resource_tags = [
        {"Key": "Name", "Value": args.name},
        {"Key": "Project", "Value": PROJECT_TAG},
    ]

    try:
        response = ec2.run_instances(
            ImageId=ami_id,
            InstanceType=args.instance_type,
            KeyName=key_pair,
            SubnetId=subnet,
            SecurityGroupIds=[sg],
            IamInstanceProfile={"Name": profile},
            UserData=user_data,
            BlockDeviceMappings=[
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
            ],
            TagSpecifications=[
                {"ResourceType": "instance", "Tags": resource_tags},
                {"ResourceType": "volume", "Tags": resource_tags},
            ],
            MinCount=1,
            MaxCount=1,
        )

        instance_id = response["Instances"][0]["InstanceId"]
        log(f"Instance launched: {instance_id}")

        log("Waiting for instance to start...")
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])

        desc = ec2.describe_instances(InstanceIds=[instance_id])
        private_ip = desc["Reservations"][0]["Instances"][0].get("PrivateIpAddress", "N/A")

    except (ClientError, WaiterError) as e:
        log(f"AWS error: {e}")
        sys.exit(1)

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
    if dcv_password:
        print(f"  Open DCV client and connect to {private_ip}")
    else:
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
