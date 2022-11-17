#!/bin/bash

cd /home/liafan/repos/uwcip-dev/twitter-apiv2-search #change directory
source venv/bin/activate

echo 'starting'
python3 download_utils.py "./samples/sample_dat/image_urls.txt" 'image' ./samples/sample_dat/
echo 'done'