import tweepy
import time
import json
import argparse
from datetime import datetime
import os
import gzip

import logging
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

def get_tweet_ids(file_name):
    tweet_ids = []
    with open(file_name, 'r') as f:
        for line in f.readlines():
            tweet_ids.append(line.strip())
    return tweet_ids

def get_hashtags(entities):
    if not entities:
        return

    hashtags = entities.get('hashtags')
    if not hashtags:
        return

    hlst = []
    for h in hashtags:
        hlst.append(h['tag'])

    return hlst

def get_expanded_urls(entities):
    if not entities:
        return

    urls = entities.get('urls')
    if not urls:
        return

    ulst = []
    for h in urls:
        ulst.append(h['expanded_url'])

    return ulst

def parse_tweet(tweet, author):
    obj = {
        "id": tweet["id"],
        "conversation_id": tweet["conversation_id"],
        "created_at": tweet["created_at"],
        "tweet": tweet["text"],
        # "hashtags": [x["tag"] for x in tweet.get("entities", {}).get("hashtags", [])],
        # "urls": [x["expanded_url"] for x in tweet.get("entities", {}).get("urls", [])],
        "hashtags": get_hashtags(tweet.get('entities')),
        "urls": get_expanded_urls(tweet.get('entities')),
        "source": tweet.get("source", None),
        "language": tweet["lang"],
        "retweet_count": tweet["public_metrics"]["retweet_count"],
        "reply_count": tweet["public_metrics"]["reply_count"],
        "like_count": tweet["public_metrics"]["like_count"],
        "quote_count": tweet["public_metrics"]["quote_count"],
        "in_reply_to_user_id": tweet.get("in_reply_to_user_id", None),
        "possibly_sensitive": tweet["possibly_sensitive"],
        "reply_settings": tweet["reply_settings"],

        "user_id": tweet["author_id"],
        "user_screen_name": author["username"],
        "user_name": author["name"],
        "user_description": author["description"],
        "user_location": author.get("location"),
        "user_created_at": author["created_at"],
        "user_followers_count": author["public_metrics"]["followers_count"],
        "user_friends_count": author["public_metrics"]["following_count"],
        "user_statuses_count": author["public_metrics"]["tweet_count"],
        "user_verified": author["verified"],

        "references": tweet.get("referenced_tweets"),
        "context_annotations": tweet.get("context_annotations", None),
    }
    return obj

import traceback
def get_tweets(api, query, tweet_fields_, user_fields_, expand_fields_, start_time_, end_time_, num_pages, fetch_context_annotation):
    if fetch_context_annotation:
        max_results=100
    else:
        max_results=500
        
    try:
        results = []
        responses = tweepy.Paginator(api.search_all_tweets,
                    query=query,
                    tweet_fields=tweet_fields_,
                    user_fields=user_fields_,
                    expansions=expand_fields_,
                    start_time=start_time_,
                    end_time=end_time_,
                    max_results=max_results, #max results per page, highest allowed is 500 
                    limit=num_pages #max number of pages to return
                  )
        
        for resp in responses: #loop through each tweepy.Response field
            #get all the users from includes
            if resp.data is None:
                continue
            # print('resp.data', resp.data)
            users = {}  # keyed by user id
            for user in resp.includes['users']:
                user_id = user["id"]
                users[user_id] = user

            #loop through regular tweets
            tweets = resp.data
            for tweet in tweets:
                author = users.get(tweet["author_id"]) #get user object
                obj = parse_tweet(tweet, author)
                # print('obj', obj)
                results.append(obj)
            time.sleep(1)#rate limit
        time.sleep(1)#rate limit
        return results
    except (TypeError, ValueError) as e:
        print('error=%s, query=%s'%(e, query))
        traceback.print_exc()
        logger.error('error=%s, query=%s'%(e, query))
        logger.error(traceback.extract_tb())
        time.sleep(10)
        return

def fetch_replies(api, tweet_id, write_file, start_time, end_time, num_pages,fetch_context_annotation):
    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    tweet_fields =  "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld,reply_settings"
    if fetch_context_annotation:
        tweet_fields += ",context_annotations"
    expansion_fields = "author_id,in_reply_to_user_id"

    query = 'conversation_id:' + str(tweet_id) #+ ' lang:en -is:retweet'
    tweets = get_tweets(api, query, tweet_fields, user_fields, expansion_fields, start_time, end_time, num_pages, fetch_context_annotation)
    if tweets:
        print("writing tweets to file, number of tweets=%s"%len(tweets))
        logger.info("writing tweets to file, number of tweets=%s"%len(tweets))
        with gzip.open(write_file, "wt") as f:
            for tweet in tweets:
                f.write(json.dumps(tweet, default=str) + "\n")
    else:
        with gzip.open(write_file, "wt") as f:
            f.write("{}")
    return

def api_test():
    credentials = get_credentials("credentials_englekri.json")
    api = get_API(credentials)
    tweet_id = "1575575350305730560"
    fetch_replies(api, tweet_id, './dat/output.txt')
    return

def batch_fetch_replies(credentials, input, output, start_time, end_time, num_pages,fetch_context_annotation):
    credentials = get_credentials(credentials)
    api = get_API(credentials)

    tweet_ids = get_tweet_ids(input)
    logger.info("fetching %s coverstations"%(len(tweet_ids)))
    for tweet_id in tweet_ids:
        write_file = os.path.join(output, "replies_%s.json.gz"%(tweet_id))
        if os.path.exists(write_file): #TODO file already exists
            continue
        print('fetching replies for tweet_id=%s and write to=%s'%(tweet_id, write_file))
        logger.info('fetching replies for tweet_id=%s and write to=%s'%(tweet_id, write_file))
        fetch_replies(api, tweet_id, write_file, start_time, end_time, num_pages,fetch_context_annotation)
    return

def main():
    parser = argparse.ArgumentParser(
        prog="stream-debug",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("credentials", metavar="CREDENTIALS.JSON", help="json file containing twitter credentials")
    parser.add_argument("tweet_ids", metavar="TWEET_IDS.CSV", help="file containing the list of tweet ids")
    parser.add_argument("output", help="output directory to store the output files")
    parser.add_argument("start_time", help="for example, '2022-09-28T00:00:00Z'")
    parser.add_argument("end_time", help="for example, '2022-10-10T00:00:00Z'")
    parser.add_argument("--fetch_context_annotation", action="store_true", default=False)
    parser.add_argument("--num_pages",default=1000)
                        
    args = parser.parse_args()

    credentials = args.credentials
    input = args.tweet_ids
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: credentials=%s, input=%s, output=%s start_time=%s end_time=%s, fetch_annotation=%s'%(credentials, input, output, args.start_time, args.end_time, args.fetch_context_annotation))

    batch_fetch_replies(credentials, input, output, args.start_time, args.end_time,args.num_pages, args.fetch_context_annotation)
    return

if __name__ == '__main__':
    # api_test()
    main()
    pass