#!/usr/bin/env bash
set -euo pipefail

# upload_sample.sh
# Usage:
#   ./upload_sample.sh --vm-host 192.168.56.101 --vm-user hadoop [--vm-port 22] [--remote-path /home/hadoop/data/] [--hdfs]
# If --hdfs is passed, script will SSH to VM and run `hdfs dfs -put` to place file into HDFS.

VM_HOST=
VM_USER=
VM_PORT=22
REMOTE_PATH="/home/hadoop/data/"
PUT_TO_HDFS=0
LOCAL_SAMPLE="$(dirname "$0")/../data_final/youtube_sample.csv"

while [[ $# -gt 0 ]]; do
  case $1 in
    --vm-host) VM_HOST="$2"; shift 2;;
    --vm-user) VM_USER="$2"; shift 2;;
    --vm-port) VM_PORT="$2"; shift 2;;
    --remote-path) REMOTE_PATH="$2"; shift 2;;
    --hdfs) PUT_TO_HDFS=1; shift 1;;
    *) echo "Unknown arg $1"; exit 1;;
  esac
done

if [[ -z "$VM_HOST" || -z "$VM_USER" ]]; then
  echo "Provide --vm-host and --vm-user" >&2
  exit 2
fi

if [[ ! -f "$LOCAL_SAMPLE" ]]; then
  echo "Local sample not found: $LOCAL_SAMPLE" >&2
  exit 3
fi

mkdir -p /tmp/hdfs_transfer
ZIP=/tmp/hdfs_transfer/$(basename "$LOCAL_SAMPLE").zip
rm -f "$ZIP"
zip -j "$ZIP" "$LOCAL_SAMPLE"

echo "Transferring $ZIP -> ${VM_USER}@${VM_HOST}:${REMOTE_PATH}"
scp -P $VM_PORT "$ZIP" ${VM_USER}@${VM_HOST}:"${REMOTE_PATH}"

if [[ $PUT_TO_HDFS -eq 1 ]]; then
  echo "Uploading into HDFS on remote host..."
  ssh -p $VM_PORT ${VM_USER}@${VM_HOST} "unzip -o ${REMOTE_PATH}/$(basename $ZIP) -d ${REMOTE_PATH} && hdfs dfs -mkdir -p /user/${VM_USER}/input && hdfs dfs -put -f ${REMOTE_PATH}/$(basename $LOCAL_SAMPLE) /user/${VM_USER}/input/"
  echo "Done: file placed in HDFS at /user/${VM_USER}/input/"
else
  echo "Transfer complete. File located at ${REMOTE_PATH}/$(basename $LOCAL_SAMPLE) on VM. Use --hdfs to automatically put into HDFS."
fi
