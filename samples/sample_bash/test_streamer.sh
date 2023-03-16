#!/bin/bash

cd ../
source venv/bin/activate
echo 'starting'

python3 stream.py credentials.json "anacortes_test.json" ./dat/anacortes/
echo 'done'