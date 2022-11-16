import os
import pandas as pd
import requests
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

def download_images(url, file_path):
    headers = get_rotating_headers()
    resp = requests.get(url, allow_redirects=True, headers=headers)
    with open(file_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return

def download_videos(url, file_path):
    #TODO, same as image; quality?; size?
    headers = get_rotating_headers()
    resp = requests.get(url, allow_redirects=True, headers=headers)
    with open(file_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return

def batch_download_images(input, output):

    return


def sample_download_image():
    # url = "https://www.liveeatlearn.com/wp-content/uploads/2018/04/carrot-on-white-1-650x411.jpg"
    url = "https://pbs.twimg.com/media/FhhP3umXEAIlzbN.jpg"
    file_name = url.split("/")[-1]
    if "." not in url[-5:]:
        file_name = file_name + ".jpg"
    file_name = os.path.join("dat", file_name)
    print(url, file_name)
    download_images(url, file_name)

def sample_download_video():
    url = "https://video.twimg.com/amplify_video/1592041571456876544/vid/480x270/y7-5V1TtuqxY2io-.mp4?tag=14"
    file_name = url.split("/")[-1] + ".mp4"
    file_name = os.path.join("dat", file_name)
    print(url, file_name)
    download_videos(url, file_name)
    return

def main():
    return

if __name__ == '__main__':
    sample_download_image()
    sample_download_video()
    pass