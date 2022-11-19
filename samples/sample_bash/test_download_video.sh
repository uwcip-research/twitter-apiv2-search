#!/bin/bash

cd /home/liafan/repos/uwcip-dev/twitter-apiv2-search #change directory
source venv/bin/activate #don't forget to create the venv first using python -m venv venv in the repo root

echo 'starting'
python3 download_media.py "./samples/sample_dat/video_urls.txt" 'video' ./samples/sample_dat/videos
echo 'done'