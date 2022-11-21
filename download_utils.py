import os
import traceback

import requests
import socket
import time

import logging
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
from logging.handlers import RotatingFileHandler
log_path = "logs/logs_download_utils_%s.txt"%(time.time())
print('log_path', log_path)
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)

from fake_useragent import UserAgent
ua = UserAgent()

def set_proxies():
    # check to see if running on our infrastructure
    if socket.gethostname() in ['earth', 'luna']:
        # set env variables for proxy to not get banned
        print("setting proxies")
        logger.info("setting proxies")
        os.environ["http_proxy"] = "http://proxy.lab.cip.uw.edu:3128"
        os.environ["https_proxy"] = "http://proxy.lab.cip.uw.edu:3128"
    return

def get_rotating_headers():
    #set header to be less bot like
    header = {
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "User-Agent": ua.random
    }
    return header

def download_image(url, file_path):
    headers = get_rotating_headers()
    resp = requests.get(url, allow_redirects=True, headers=headers)
    with open(file_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return

def download_video(url, file_path):
    headers = get_rotating_headers()
    resp = requests.get(url, allow_redirects=True, stream=True, headers=headers)
    with open(file_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return

def batch_download(urls, output, input_type, sleep_time=5):
    print('total number of urls', len(urls))
    max_retry = 3
    for url in urls:
        if not url.startswith("http"):
            continue
        file_name = url.split("/")[-1]
        file_extension = ".jpg" if input_type=='image' else '.mp4'
        if "." not in url[-5:]:
            file_name = file_name + file_extension
        file_name = os.path.join(output, file_name)
        if os.path.exists(file_name):
            continue
        retry = 0
        while True:
            try:
                print('downloading', url, file_name)
                if input_type == 'image':
                    download_image(url, file_name)
                else:
                    download_video(url, file_name)
                time.sleep(sleep_time)
                break
            except Exception as e:
                print('error downloading', e, url, file_name)
                traceback.print_exc()
                logger.error(e)
                logger.error(traceback.extract_tb())
                time.sleep(60 * (retry+1))
                if retry>=max_retry:
                    break
                retry+=1
                continue
    return

def sample_download_image():
    url = "https://pbs.twimg.com/media/FhhP3umXEAIlzbN.jpg"
    file_name = url.split("/")[-1]
    if "." not in url[-5:]:
        file_name = file_name + ".jpg"
    file_name = os.path.join("dat", file_name)
    print(url, file_name)
    download_image(url, file_name)

def sample_download_video():
    url = "https://video.twimg.com/amplify_video/1592041571456876544/vid/480x270/y7-5V1TtuqxY2io-.mp4?tag=14"
    file_name = url.split("/")[-1] + ".mp4"
    file_name = os.path.join("dat", file_name)
    print(url, file_name)
    download_video(url, file_name)
    return

def sample_downloads():
    set_proxies()
    sample_download_image()
    sample_download_video()
    return