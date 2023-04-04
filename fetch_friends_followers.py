import argparse
import json
import logging
import os
import time
import traceback

import tweepy

logging.captureWarnings(True)
logger = logging.getLogger(__name__)
from logging.handlers import RotatingFileHandler
log_path = "logs/logs_fetch_tweet_replies_%s.txt"%(time.time())
print('log_path', log_path)
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)

def get_credentials(file_name):
    with open(file_name, 'r') as f:
        credentials = json.load(f)
        return credentials

def get_API(credentials):
    client = tweepy.Client(bearer_token=credentials['bearer_token'], wait_on_rate_limit=True)
    return client

def get_user_ids(file_name):
    user_ids = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            user_ids.append(line.strip())
    return user_ids

def get_friends_followers(api, user_id, api_func):
    all_user_ids = []
    max_retries = 3
    retry_count = 0
    pagination_token = None
    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    while True:
        try:
            resps = tweepy.Paginator(api_func,
                                     id=user_id,
                                     pagination_token=pagination_token,
                                     user_fields=user_fields,
                                     max_results=1000, #max per request is 1K
                                     limit=500)  # set max followers to 500K #TODO set to use configuration
            for resp in resps:
                if resp is None or resp.data is None:
                    break
                print(resp.meta)
                if "next_token" in resp.meta:
                    pagination_token = resp.meta['next_token']
                else:
                    pagination_token = None

                networked_users = resp.data
                for networked_user in networked_users:
                    all_user_ids.append(json.dumps(networked_user.data))

            return all_user_ids
        except Exception as e:
            print('>>>>>>>>>>>>>>>>>>>>>Error', e)
            traceback.print_exc()
            logger.error("error=%s" % (e))
            if retry_count >= max_retries:
                return
            retry_count += 1
            time.sleep(60 * (retry_count + 1))
            continue

def fetch_friends_follower(credentials, input, output, friends, followers):
    credentials = get_credentials(credentials)
    api = get_API(credentials)
    user_ids = get_user_ids(input)

    if friends:
        print('fetching friends')
        for user_id in user_ids:
            output_file = os.path.join(output, "%s_friends.csv"%user_id)
            if os.path.exists(output_file):
                continue
            friends = get_friends_followers(api, user_id, api.get_users_following)
            if friends:
                with open(output_file, 'wt') as f:
                    f.write("\n".join(friends))

    #TODO, reduce duplicate code
    if followers:
        print('fetching followers')
        for user_id in user_ids:
            output_file = os.path.join(output, "%s_followers.csv"%user_id)
            if os.path.exists(output_file):
                continue
            followers = get_friends_followers(api, user_id, api.get_users_followers)
            if followers:
                with open(output_file, 'wt') as f:
                    f.write("\n".join(followers))
    return

def main():
    parser = argparse.ArgumentParser(
        prog="stream-debug",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("credentials", metavar="CREDENTIALS.JSON", help="json file containing twitter credentials")
    parser.add_argument("user_ids", metavar="USER_IDS.CSV", help="file containing the list of user ids")
    parser.add_argument("output", help="output directory to store the output files")
    parser.add_argument("--fetch-friends", dest="fetch_friends", action="store_true", help="set this flag to fetch all friends for each account")
    parser.add_argument("--fetch-followers", dest="fetch_followers", action="store_true", help="set this flag to fetch all followers for each account")

    args = parser.parse_args()
    credentials = args.credentials
    input = args.user_ids
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    friends, followers = args.fetch_friends, args.fetch_followers

    print('args: credentials=%s, input=%s, output=%s, friends=%s, followers=%s' % (credentials, input, output, friends, followers))
    fetch_friends_follower(credentials, input, output, friends, followers)
    return


if __name__ == '__main__':
    # api_test()
    main()
    pass


