#!/bin/bash

cd /home/liafan/repos/uwcip-dev/twitter-apiv2-search #change directory
source venv/bin/activate

echo 'starting'
python3 download_utils.py "./samples/sample_dat/video_urls.txt" 'video' ./samples/sample_dat/videos
echo 'done'