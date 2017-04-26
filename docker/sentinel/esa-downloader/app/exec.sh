#!/bin/bash
echo "$@"
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts "$@"
