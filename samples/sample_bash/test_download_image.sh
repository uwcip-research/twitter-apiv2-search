#!/bin/bash

cd /home/liafan/repos/uwcip-dev/twitter-apiv2-search #change directory
source venv/bin/activate #don't forget to create the venv first using python -m venv venv in the repo root

echo 'starting'
python3 download_media.py "./samples/sample_dat/image_urls.txt" 'image' ./samples/sample_dat/
echo 'done'