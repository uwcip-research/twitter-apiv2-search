import os
import time

import pandas as pd
import requests
import argparse
import tenacity

import socket
print(socket.gethostname())
if socket.gethostname()=="earth":
    #set env variables for proxy to not get banned
    os.environ["http_proxy"] = "http://proxy.lab.cip.uw.edu:3128"
    os.environ["https_proxy"] = "http://proxy.lab.cip.uw.edu:3128"

from fake_useragent import UserAgent
ua = UserAgent()

def get_rotating_headers():
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
    #TODO, same as image; quality?; size?
    headers = get_rotating_headers()
    resp = requests.get(url, allow_redirects=True, headers=headers)
    with open(file_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return

def read_urls(file_name):
    urls = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            if line.startswith("http"):
                urls.append(line.strip())
    return urls

def batch_download(input, output, input_type):
    urls = read_urls(input)
    print('total number of urls', len(urls))
    max_retry = 3
    for url in urls:
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
                time.sleep(2)
                break
            except Exception as e:
                print('error downloading', e, url, file_name)
                time.sleep(60)
                if retry>=max_retry:
                    break
                retry+=1
                continue
    return

def sample_download_image():
    # url = "https://www.liveeatlearn.com/wp-content/uploads/2018/04/carrot-on-white-1-650x411.jpg"
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

def sample():
    sample_download_image()
    sample_download_video()
    return

def main():
    parser = argparse.ArgumentParser(
        prog="stream2",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", metavar="INPUT.TXT", help="file containing urls for downloading")
    parser.add_argument("type", help="url type: 1) image or 2) video")
    parser.add_argument("output", help="output directory to store the output files")

    args = parser.parse_args()

    input = args.input
    input_type = args.type
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: input=%s, input_type=%s, output=%s' % (input, input_type, output))
    batch_download(input, output, input_type)
    return

if __name__ == '__main__':
    main()
    pass