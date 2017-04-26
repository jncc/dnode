#!/bin/bash
echo "some shit"
echo "$@"
echo "command"
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts "$@"
