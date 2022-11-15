import os
import pandas as pd
import requests
import socket
print(socket.gethostname())
if socket.gethostname()=="earth":
    #set env variables for proxy to not get banned
    os.environ["http_proxy"] = "http://proxy.lab.cip.uw.edu:3128"
    os.environ["https_proxy"] = "http://proxy.lab.cip.uw.edu:3128"

def download_images(url):

    return

def download_videos(url):
    #TODO
    return

def batch_download_images(input, output):

    return

def main():

    return

if __name__ == '__main__':
    pass