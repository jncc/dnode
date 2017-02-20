#!/bin/bash

python process.py

# Stress test
# stress-ng --cpu 2 --io 2 --vm 1 --vm-bytes 1G --timeout 15m --metrics-brief > /mnt/state/output.txt

chown -R $USERID:$GROUPID /mnt/state
