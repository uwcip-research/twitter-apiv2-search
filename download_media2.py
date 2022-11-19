import os
import argparse
import traceback

import pandas as pd

from download_utils import batch_download, set_proxies
import time

import logging
logger = logging.getLogger(__name__)
from logging.handlers import RotatingFileHandler
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
log_path = "logs/logs_download_media2_%s.txt"%(time.time())
print('log_path', log_path)
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)

def get_images(media):
    if not media or isinstance(media, float):
        return
    results = []
    for m in media:
        if m['media_type'] == 'photo':
            if 'media_url' in m:
                results.append(m['media_url'])
    if results:
        return results

def get_videos(media):
    if not media or isinstance(media, float):
        return
    results = []
    for m in media:
        if m['media_type'] == 'video' and 'media_variants' in m:
            media_variants = m['media_variants']
            for subm in media_variants:
                if 'url' in subm:
                    results.append(subm['url'])
    if results:
        return results
    return

def parse_media_urls(input, func):
    results = []
    for file in os.listdir(input):
        if not file.endswith(".json.gz"):
            continue
        try:
            file_path = os.path.join(input, file)
            print(file_path)
            df = pd.read_json(file_path, compression='gzip', lines=True)
            df = df[~df['media_objects'].isnull()]
            df['urls'] = df['media_objects'].apply(func)
            df = df[~df['urls'].isnull()]
            df = df[['urls']]
            df = df.explode('urls')
            results.append(df)
        except Exception as e:
            print(e)
            traceback.print_exc()
            logger.error(e)
    df = pd.concat(results, axis=0)
    df.drop_duplicates(inplace=True)
    return df['urls'].to_list()

def loop_download(input, output, input_type):
    while True:
        try:
            if input_type == 'image':
                urls = parse_media_urls(input, get_images)
                batch_download(urls, output, input_type)
            elif input_type == 'video':
                urls = parse_media_urls(input, get_videos)
                batch_download(urls, output, input_type)
            elif input_type == 'both':
                urls1 = parse_media_urls(input, get_images)
                batch_download(urls1, output, 'image')
                urls2 = parse_media_urls(input, get_videos)
                batch_download(urls2, output, 'video')
        except Exception as e:
            traceback.print_exc()
            logger.error(e)
        time.sleep(60) #sleep for a day
    return

def main():
    #TODO, I only tested this script using twitter data
    #Periodically checks an input folder to see if new images need to be fetched
    parser = argparse.ArgumentParser(
        prog="download_media_cron",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", metavar="INPUT", help="input directory containing all the downloaded tweet data using search2.py")
    parser.add_argument("type", help="url type: 1) image or 2) video or 3) both")
    parser.add_argument("output", help="output directory to store the output files")

    args = parser.parse_args()

    input = args.input
    input_type = args.type
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: input=%s, input_type=%s, output=%s' % (input, input_type, output))
    loop_download(input, output, input_type)
    return

if __name__ == '__main__':
    # sample_downloads()
    main()
    pass