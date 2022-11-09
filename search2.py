import tweepy
import time
import json
import argparse
from datetime import datetime
import os
import gzip

def get_json(file_name):
    with open(file_name, 'r') as f:
        jobj = json.load(f)
        return jobj

def get_API(credentials):
    client = tweepy.Client(bearer_token=credentials['bearer_token'], wait_on_rate_limit=True)
    return client

def parse_tweet(tweet, author):
    obj = {
        "id": tweet["id"],
        "conversation_id": tweet["conversation_id"],
        "created_at": tweet["created_at"],
        "tweet": tweet["text"],
        "hashtags": [x["tag"] for x in tweet.get("entities", {}).get("hashtags", [])],
        "urls": [x["expanded_url"] for x in tweet.get("entities", {}).get("urls", [])],
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

def write_to_file(results, output, timestamp, partition_idx):
    write_file = os.path.join(output, "partition_%s_%s.json.gz" % (partition_idx, timestamp))
    with gzip.open(write_file, "wt") as f:
        for tweet in results:
            f.write(json.dumps(tweet, default=str) + "\n")
    return

import traceback
def get_tweets(api, query, output, tweet_fields_, user_fields_, expand_fields_):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    lines_per_file = 5 #for testing
    partition_idx = 0
    try:
        results = []
        responses = tweepy.Paginator(api.search_all_tweets,
                                     query=query['query'],
                                     tweet_fields=tweet_fields_,
                                     user_fields=user_fields_,
                                     expansions=expand_fields_,
                                     start_time=query['start_time'],
                                     end_time=query['end_time'],
                                     max_results=query['max_results'],  # max results per page, highest allowed is 500
                                     limit=query['max_pages']  # max number of pages to return
                                     )

        for resp in responses:  # loop through each tweepy.Response field
            # get all the users from includes
            users = {}  # keyed by user id
            for user in resp.includes['users']:
                user_id = user["id"]
                users[user_id] = user

            # loop through regular tweets
            tweets = resp.data
            for tweet in tweets:
                author = users.get(tweet["author_id"])  # get user object
                obj = parse_tweet(tweet, author)
                results.append(obj)

            #write to file
            if len(results)>= lines_per_file:
                write_to_file(results, output, timestamp, partition_idx)
                results = []
                partition_idx +=1

        #write the remaining results
        if len(results)>0:
            write_to_file(results, output, timestamp, partition_idx)

    except (TypeError, ValueError) as e:
        print('error', e)
        traceback.print_exc()
        time.sleep(10)
        return

def batch_fetch(credentials_file, query_file, output):
    credentials = get_json(credentials_file)
    api = get_API(credentials)
    query = get_json(query_file)
    print(query)


    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    tweet_fields =  "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld,reply_settings,context_annotations"
    place_fields = "contained_within,country,country_code,full_name,geo,id,name,place_type"
    expansion_fields = "author_id,in_reply_to_user_id"

    get_tweets(api, query, output, tweet_fields, user_fields, expansion_fields)
    return

def api_test():
    credentials_file = "credentials_englekri.json"
    query_file = "dat/sample_query_file.json"
    output = "./dat"
    batch_fetch(credentials_file, query_file, output)
    return

def main():
    parser = argparse.ArgumentParser(
        prog="stream-debug",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("credentials", metavar="CREDENTIALS.JSON", help="json file containing twitter credentials")
    parser.add_argument("query", metavar="QUERY.TXT", help="file containing the search query")
    parser.add_argument("output", help="output directory to store the output files")

    args = parser.parse_args()

    credentials_file = args.credentials
    query_file = args.query
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: credentials=%s, query_file=%s, output=%s' % (credentials_file, query_file, output))

    batch_fetch(credentials_file, query_file, output)
    return


if __name__ == '__main__':
    api_test()
    # main()
    pass