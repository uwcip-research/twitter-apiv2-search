import sys

import numpy as np
import tweepy
import time
import json
import argparse
from datetime import datetime
import os
import gzip
import tenacity
from tweepy import Response

import logging
logging.captureWarnings(True)
logger = logging.getLogger(__name__)
from logging.handlers import RotatingFileHandler
log_path = "logs/logs_fetch_tweets_by_ids_%s.txt"%(time.time())
print('log_path', log_path)
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)

def get_json(file_name):
    with open(file_name, 'r') as f:
        jobj = json.load(f)
        return jobj

def get_API(credentials):
    client = tweepy.Client(bearer_token=credentials['bearer_token'], wait_on_rate_limit=True)
    return client


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

def get_media_view_count(media):
    if 'public_metrics' not in media:
        return
    return media.get("public_metrics").get("view_count", None)

def parse_ref_tweet(tweet):
    # print('>>>>>>>>>>>>>>>. ref tweet', tweet.data)
    obj = {
        "id": tweet["id"],
        "conversation_id": tweet["conversation_id"],
        "created_at": tweet["created_at"],
        "tweet": tweet["text"],
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
    }
    return obj

def parse_tweet(tweet, users, **kwargs):
    author = users.get(tweet["author_id"])  # get user object
    obj = {
        "id": tweet["id"],
        "conversation_id": tweet["conversation_id"],
        "created_at": tweet["created_at"],
        "tweet": tweet["text"],
        "entities": tweet.entities,
        # "hashtags": [x.get("tag") for x in tweet.get("entities", {}).get("hashtags", [{}])],
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

    if kwargs['includes_tweets']:
        includes_tweets = kwargs['includes_tweets']
        if tweet['referenced_tweets']:
            ref_tweets = tweet.get("referenced_tweets")
            for ref_tweet_dict in ref_tweets:
                if ref_tweet_dict['id'] in includes_tweets:
                    ref_tweet_obj = parse_ref_tweet(includes_tweets[ref_tweet_dict['id']])
                    type = ref_tweet_dict['type']
                    obj['references_%s'%type] = ref_tweet_obj

    if kwargs['includes_media']:
        includes_media = kwargs['includes_media']
        if "attachments" in tweet and "media_keys" in tweet['attachments']:
            media_keys = tweet['attachments']['media_keys']
            mobjs = []
            for media_key in media_keys:
                media = includes_media.get(media_key)
                if not media:
                    continue
                # print('>>>>>>>>>>>>>media', media.data, media.get("public_metrics", {}))
                mobj = {
                    "media_key": media["media_key"],
                    "media_type": media["type"],
                    "media_view_count": get_media_view_count(media),
                    "media_height": media.get("height"),
                    "media_width": media.get("width"),
                    "media_url": media.get("url"),
                    "media_preview_image_url": media.get("preview_image_url"),
                    "media_variants":media.get("variants"),
                    "media_alt_text": media.get("alt_text")
                }
                mobjs.append(mobj)
            obj['media_objects'] = mobjs
    return obj

def write_to_file(results, output, timestamp, job_name, idx):
    write_file = os.path.join(output, "%s_%s_%s.json.gz" % (job_name, timestamp, idx))
    print('writing to file', write_file)
    with gzip.open(write_file, "wt") as f:
        for tweet in results:
            f.write(json.dumps(tweet, default=str, ensure_ascii=False) + "\n")
    return

def get_tweet_ids(file_name):
    with open(file_name, 'r') as f:
        ids = [x.strip() for x in f.readlines()]
        return ids

def chunk_it(lst, chunk_size=100):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

import traceback
def get_tweets(credentials, query, input, output, tweet_fields_, user_fields_, expand_fields_, place_fields_, media_fields_):
    api = get_API(credentials)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    job_name = query.get('name', 'default')
    lines_per_file = query.get('lines_per_file', 10000) #for testing
    tweet_ids_all = get_tweet_ids(input)
    tweet_ids_lst = chunk_it(tweet_ids_all)
    print('number of tweets to fetch', len(tweet_ids_all), tweet_ids_all[:5])
    logger.info('number of tweets to fetch=%s'%len(tweet_ids_all))

    max_retries = 3
    results = []
    write_part = 0
    for idx, tweet_ids in enumerate(tweet_ids_lst):
        retry_count = 0
        while True:
            try:
                resp = api.get_tweets(
                    ids=tweet_ids,
                    tweet_fields=tweet_fields_,
                    user_fields=user_fields_,
                    expansions=expand_fields_,
                    place_fields=place_fields_,
                    media_fields=media_fields_,
                )

                # get all the users from includes
                users = {}  # keyed by user id
                for user in resp.includes['users']:
                    user_id = user["id"]
                    users[user_id] = user

                includes_media = {}
                if 'media' in resp.includes:
                    for media in resp.includes['media']:
                        media_key = media['media_key']
                        includes_media[media_key] = media

                # extract include tweets
                includes_tweets = {}
                if "tweets" in resp.includes:
                    tlist = resp.includes['tweets']
                    for tweet in tlist:
                        includes_tweets[tweet.id] = tweet

                #TODO probably needs to include logic for place/location too

                # loop through regular tweets
                tweets = resp.data
                for tweet in tweets:
                    try:
                        obj = parse_tweet(tweet, users, includes_tweets=includes_tweets, includes_media=includes_media)
                        results.append(obj)
                    except Exception as e:
                        print(">>>>>Error Parsing Tweet", e, tweet.data)
                        logger.error("error=%s, tweet=%s"%(e, tweet.data))
                        traceback.print_exc()

                #write to file
                if len(results)>= lines_per_file:
                    write_to_file(results, output, timestamp, job_name, write_part)
                    results = []
                    write_part+=1
                break
            except Exception as e:
                print('>>>>>>>>>>>>>>>>>>>>>Error', e)
                logger.error("error=%s"%(e))
                if retry_count>=max_retries:
                    return
                retry_count+=1
                time.sleep(60 * (retry_count+1))
                api = get_API(credentials)
                continue

    # write the remaining results
    if len(results) > 0:
        write_to_file(results, output, timestamp, job_name, write_part)

def batch_fetch(credentials_file, query_file, input, output):
    credentials = get_json(credentials_file)
    query = get_json(query_file)
    print(query)
    logger.info('query=%s'%query)

    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    user_fields = query.get('user_fields', user_fields)

    tweet_fields =  "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld,reply_settings,context_annotations"
    tweet_fields = query.get('tweet_fields', tweet_fields)

    expansion_fields = "author_id,in_reply_to_user_id,referenced_tweets.id"
    expansion_fields = query.get("expansion_fields", expansion_fields)

    place_fields = ""
    if query.get('include_place', False):
        place_fields = "contained_within,country,country_code,full_name,geo,id,name,place_type"
        place_fields = query.get('place_fields', place_fields)

    media_fields = ""
    if query.get('include_media', False):
        media_fields = "media_key,type,url,duration_ms,height,width,public_metrics,alt_text,variants"
        media_fields = query.get('media_fields', media_fields)

    print('user_fields', user_fields)
    print('tweet_fields', tweet_fields)
    print('expansion_fields', expansion_fields)
    print('place_fields', place_fields)
    print('media_fields', media_fields)

    get_tweets(credentials, query, input, output, tweet_fields, user_fields, expansion_fields, place_fields, media_fields)
    return

def api_test():
    credentials_file = "credentials_englekri.json"
    query_file = "samples/sample_query_files/sample_query_file.json"
    output = "./dat"
    batch_fetch(credentials_file, query_file, output)
    return

def main():
    #This is a more flexible version of search.py; also uses tweepy here
    parser = argparse.ArgumentParser(
        prog="fetch_tweets_by_ids",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("credentials", metavar="CREDENTIALS.JSON", help="json file containing twitter credentials")
    parser.add_argument("query", metavar="QUERY.TXT", help="file containing query configurations")
    parser.add_argument("input", help="input file containing tweet ids")
    parser.add_argument("output", help="output directory to store the output files")

    args = parser.parse_args()

    credentials_file = args.credentials
    query_file = args.query
    output = args.output
    input = args.input
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: credentials=%s, query_file=%s, input=%s, output=%s' % (credentials_file, query_file, input, output))
    logger.info('args: credentials=%s, query_file=%s, input=%s, output=%s' % (credentials_file, query_file, input, output))

    batch_fetch(credentials_file, query_file, input, output)
    return

if __name__ == '__main__':
    # api_test()
    main()
    pass