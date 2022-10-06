import tweepy
import time
import json
import argparse
from datetime import datetime
import os

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

# {"source": "Twitter for Android", "in_reply_to_user_id": "818893114979061761", "author_id": "1555759725605752832", "possibly_sensitive": false, "conversation_id": "1575575350305730560", "public_metrics": {"retweet_count": 0, "reply_count": 0, "like_count": 1, "quote_count": 0}, "id": "1575636478520045569", "lang": "en", "created_at": "2022-09-29T23:59:45.000Z", "edit_history_tweet_ids": ["1575636478520045569"], "entities": {"mentions": [{"start": 0, "end": 13, "username": "JoJoFromJerz", "id": "818893114979061761"}]}, "text": "@JoJoFromJerz Your a clown!", "referenced_tweets": [{"type": "replied_to", "id": "1575575350305730560"}]}

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
def get_tweets(api, query, tweet_fields_, user_fields_, expand_fields_, start_time_, end_time_, num_pages=10):
    try:
        results = []

        responses = tweepy.Paginator(api.search_all_tweets,
                    query=query,
                    tweet_fields=tweet_fields_,
                    user_fields=user_fields_,
                    expansions=expand_fields_,
                    start_time=start_time_,
                    end_time=end_time_,
                    max_results=10, #max results per page
                    limit=num_pages #max number of pages to return
                  )

        for resp in responses: #loop through each tweepy.Response field
            #get all the users from includes
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

        return results
    except (TypeError, ValueError) as e:
        print('error', e)
        traceback.print_exc()
        time.sleep(10)
        return

def fetch_replies(api, tweet_id, write_file):
    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    tweet_fields =  "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld"
    place_fields = "contained_within,country,country_code,full_name,geo,id,name,place_type"
    expansion_fields = "author_id,in_reply_to_user_id"

    # Replace with time period of your choice
    start_time = '2022-09-28T00:00:00Z'

    # Replace with time period of your choice
    end_time = '2022-10-06T00:00:00Z'

    # Replace with the maximum number of pages of your choice
    num_pages=1

    query = 'conversation_id:' + str(tweet_id) + ' lang:en -is:retweet'
    print(query)

    tweets = get_tweets(api, query, tweet_fields, user_fields, expansion_fields, start_time, end_time, num_pages)
    if tweets:
        print("writing tweets to file, number of tweets=%s"%len(tweets))
        with open(write_file, "wt") as f:
            for tweet in tweets:
                f.write(json.dumps(tweet, default=str) + "\n")
    return

def api_test():
    credentials = get_credentials("englekri_credentials.json")
    api = get_API(credentials)
    tweet_id = "1575575350305730560"
    fetch_replies(api, tweet_id, './dat/output.txt')
    return

def batch_fetch_replies(credentials, input, output):
    credentials = get_credentials(credentials)
    api = get_API(credentials)

    tweet_ids = get_tweet_ids(input)
    for tweet_id in tweet_ids:
        write_file = os.path.join(output, "replies_%s.txt"%tweet_id)
        print('fetching replies for tweet_id=%s and write to=%s'%(tweet_id, write_file))
        fetch_replies(api, tweet_id, write_file)
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
    args = parser.parse_args()

    credentials = args.credentials
    input = args.tweet_ids
    output = args.output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    print('args: credentials=%s, input=%s, output=%s timestamp=%s'%(credentials, input, output, timestamp))

    #TODO fetch all replies
    batch_fetch_replies(credentials, input, output)
    return

if __name__ == '__main__':
    # api_test()
    main()
    pass