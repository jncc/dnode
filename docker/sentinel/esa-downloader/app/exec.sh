#!/bin/bash
rm -rf /mnt/state/*
PYTHONPATH='.' luigi --module workflow DownloadAvailableProducts "$@"
