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
import sys
import time
from datetime import datetime, timezone

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

BUILDER_INSTANCE_TYPE = "t3.medium"
POLL_INTERVAL_SECONDS = 60
SETUP_TIMEOUT_SECONDS = 45 * 60  # 45 minutes


def launch_builder(ec2, ami: str, subnet: str, sg: str,
                   key_pair: str, profile: str, user_data: str) -> str:
    """Launch the temporary builder instance. Returns instance ID."""
    tags = [
        {"Key": "Name", "Value": "cosmx-ami-builder"},
        {"Key": "Project", "Value": PROJECT_TAG},
    ]
    response = ec2.run_instances(
        ImageId=ami,
        InstanceType=BUILDER_INSTANCE_TYPE,
        KeyName=key_pair,
        SubnetId=subnet,
        SecurityGroupIds=[sg],
        IamInstanceProfile={"Name": profile},
        UserData=user_data,
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": ROOT_VOLUME_GB, "VolumeType": "gp3"},
        }],
        TagSpecifications=[
            {"ResourceType": "instance", "Tags": tags},
            {"ResourceType": "volume", "Tags": tags},
        ],
        MinCount=1,
        MaxCount=1,
    )
    return response["Instances"][0]["InstanceId"]


def poll_setup_completion(ssm, instance_id: str) -> bool:
    """Poll via SSM for the sentinel file indicating setup is complete."""
    log("Waiting for AMI setup to complete (this takes 15-30 minutes)...")
    start = time.monotonic()

    while time.monotonic() - start < SETUP_TIMEOUT_SECONDS:
        elapsed = int(time.monotonic() - start)
        minutes = elapsed // 60
        print(f"\r  [{minutes}m elapsed] Checking setup status...", end="", flush=True)

        try:
            send_response = ssm.send_command(
                InstanceIds=[instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": [
                    "test -f /var/lib/cloud/instance/ami-setup-complete && echo READY || echo PENDING"
                ]},
            )
            command_id = send_response["Command"]["CommandId"]
        except ClientError:
            # SSM agent may not be ready yet — wait and retry
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        time.sleep(10)

        try:
            invocation = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
            if invocation.get("StandardOutputContent", "").strip() == "READY":
                print()
                return True
        except ClientError:
            pass  # command not yet complete

        time.sleep(POLL_INTERVAL_SECONDS)

    print()
    return False


def create_image(ec2, instance_id: str, ami_name: str) -> str:
    """Stop the instance and create an AMI from it. Returns AMI ID."""
    log("Stopping builder instance for clean snapshot...")
    ec2.stop_instances(InstanceIds=[instance_id])
    ec2.get_waiter("instance_stopped").wait(InstanceIds=[instance_id])

    log(f"Creating AMI: {ami_name}")
    response = ec2.create_image(
        InstanceId=instance_id,
        Name=ami_name,
        Description="CosMx analytics: R + RStudio + UV + Python + Jupyter + DCV",
    )
    ami_id = response["ImageId"]

    log(f"Waiting for AMI {ami_id} to become available...")
    ec2.get_waiter("image_available").wait(ImageIds=[ami_id])
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

    session = boto_session(region)
    ec2 = session.client("ec2")
    ssm = session.client("ssm")

    ami_name = args.name or f"cosmx-analytics-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    user_data = AMI_SETUP_SCRIPT.read_text()

    log("Building CosMx analytics AMI")
    log(f"  Base AMI:       {base_ami}")
    log(f"  Builder type:   {BUILDER_INSTANCE_TYPE}")
    log(f"  AMI name:       {ami_name}")

    instance_id = launch_builder(ec2, base_ami, subnet, sg, key_pair, profile, user_data)
    log(f"Builder instance launched: {instance_id}")

    try:
        log("Waiting for instance to reach 'running' state...")
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        log("Builder instance is running")

        if not poll_setup_completion(ssm, instance_id):
            log("ERROR: AMI setup timed out.")
            log(f"Debug: aws ssm start-session --target {instance_id}")
            log("Check /var/log/ami-setup.log for details")
            log(f"Builder instance {instance_id} left running for debugging")
            sys.exit(1)

        log("AMI setup completed successfully")

        ami_id = create_image(ec2, instance_id, ami_name)
        log(f"AMI created: {ami_id}")

        if args.keep_builder:
            log(f"Builder instance kept: {instance_id}")
        else:
            log("Terminating builder instance...")
            ec2.terminate_instances(InstanceIds=[instance_id])
            log("Builder instance terminated")

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

    except (ClientError, WaiterError) as e:
        log(f"AWS error: {e}")
        log(f"Builder instance {instance_id} may still be running.")
        sys.exit(1)


if __name__ == "__main__":
    main()
