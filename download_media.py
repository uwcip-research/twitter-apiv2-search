import os
import argparse
from download_utils import batch_download, set_proxies
import time

import logging
logger = logging.getLogger(__name__)
from logging.handlers import RotatingFileHandler
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
log_path = "logs/logs_download_media_%s.txt"%(time.time())
print('log_path', log_path)
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)

def read_urls(file_name):
    urls = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            if line.startswith("http"):
                urls.append(line.strip())
    return urls

def main():
    #TODO, I only tested this script using twitter data
    parser = argparse.ArgumentParser(
        prog="download_media",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", metavar="INPUT.TXT", help="file containing urls for downloading")
    parser.add_argument("type", help="url type: 1) image or 2) video")
    parser.add_argument("output", help="output directory to store the output files")
    parser.add_argument("-c", "--cron", action="store_true",help="periodically loop through the input directory and fetch new images or videos")

    args = parser.parse_args()

    input = args.input
    input_type = args.type
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: input=%s, input_type=%s, output=%s' % (input, input_type, output))

    #set proxies servers; only works if running on CIP infrastructure
    set_proxies()

    urls = read_urls(input)
    batch_download(urls, output, input_type)
    return

if __name__ == '__main__':
    # sample_downloads()
    main()
    pass