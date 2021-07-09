"""
You can run this program like this:

python3 search.py --starting="2016-09-01T00:00:00Z" --stopping="2021-02-11T00:00:00Z" --query="from:ChildrensHD" > searches/search_childrenshd.json
"""

import argparse
import json
import logging
import os
import requests
import sys
import time
import traceback
from glob import glob

logger = logging.getLogger(__name__)



def fetch(bearer_token, query, starting, stopping, next_token=None):
    try:
        params = {
            # query must not be greater than 1024 characters FYI
            "query": query,

            # limit to ten results (minimum 10, maximum 500)
            # this limit is exclusive (i.e. 10 gets you 9)
            "max_results": 100,

            # "expand" every field possible. this takes id numbers that appear in a
            # tweet and turns them in actual readable text.
            "expansions": "author_id,referenced_tweets.id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id",

            # fill out these fields
            "user.fields": "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld",
            "tweet.fields": "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
            "place.fields": "contained_within,country,country_code,full_name,geo,id,name,place_type",
        }

        # time box to these times, exclusive (ISO8601, "YYYY-MM-DDTHH:mm:ssZ")
        # example timestamp: "2020-01-01T00:00:00Z"
        if starting is not None:
            params["start_time"] = starting
        if stopping is not None:
            params["end_time"] = stopping

        if next_token is not None:
            params["next_token"] = next_token

        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        r = requests.get("https://api.twitter.com/2/tweets/search/all", params=params, headers=headers)

        if r.status_code >= 500:
            logger.error("received internal server error ({}) from Twitter API".format(r.status_code))
            return

        if r.status_code == 400:
            raise RuntimeError("your search query was invalid: {}".format(r.text))

        try:
            requests_remaining = int(r.headers.get("x-rate-limit-remaining"))
            seconds_remaining  = int(r.headers.get("x-rate-limit-reset")) - int(time.time())
            logger.info("api status: requests remaining = {}, seconds remaining = {}".format(requests_remaining, seconds_remaining))
        except (TypeError, ValueError) as e:
            logger.warning("error processing rate limit values: {}".format(e))
            logger.warning(r.headers)
            logger.warning(r.text)
            time.sleep(10)
            return

        if r.status_code == 429:
            logger.error("reached rate limit, sleeping for {} seconds".format(seconds_remaining))
            time.sleep(seconds_remaining + 1)
            return
        else:
            time.sleep(1)

        return r.json()
    except json.decoder.JSONDecodeError as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())


def parse(raw):
    users = {}  # keyed by user id
    for user in raw.get("includes", {}).get("users", []):
        user_id = user["id"]
        users[user_id] = user

    linked_tweets = {}  # keyed by tweet id
    for tweet in raw.get("includes", {}).get("tweets", []):
        tweet_id = tweet["id"]
        linked_tweets[tweet_id] = tweet

    for tweet in raw.get("data", []) + list(linked_tweets.values()):
        author = users.get(tweet["author_id"])

        obj = {
            "id": tweet["id"],
            "conversation_id": tweet["conversation_id"],
            "created_at": tweet["created_at"],
            "tweet": tweet["text"],
            "hashtags": [x["tag"] for x in tweet.get("entities", {}).get("hashtags", [])],
            "urls": [x["expanded_url"] for x in tweet.get("entities", {}).get("urls", [])],
            "source": tweet["source"],
            "language": tweet["lang"],
            "retweet_count": tweet["public_metrics"]["retweet_count"],
            "reply_count": tweet["public_metrics"]["reply_count"],
            "like_count": tweet["public_metrics"]["like_count"],
            "quote_count": tweet["public_metrics"]["quote_count"],
            "in_reply_to_user_id": tweet.get("in_reply_to_user_id", None),

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
        }
        print(json.dumps(obj))

    return len(raw.get("data", []))


def main(**kwargs):
    # load credentials
    bearer_token = None
    with open("academic_credentials.json", "rt") as f:
        credentials = json.load(f)
        bearer_token = credentials.get("bearer_token")
        if bearer_token is None:
            raise RuntimeError("could not load bearer token from credentials.json")

    pages = 0
    total = 0
    next_token = None
    while True:
        results = fetch(bearer_token, kwargs["query"], kwargs["starting"], kwargs["stopping"], next_token)
        if results is None:
            continue  # try the page again

        try:
            total = total + parse(results)

            # is there more data?
            if "meta" in results and results["meta"].get("next_token"):
                next_token = results["meta"]["next_token"]
                pages = pages + 1
            else:
                logger.info("finished fetching {} tweets".format(total))
                break  # break the loop and go to the next
        except Exception as e:
            logger.error("GENERAL EXCEPTION: {}".format(e))
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="search",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("query", metavar="QUERY", help="twitter APIv2 query to search")
    parser.add_argument("--starting", help="the time to start the search (YYYY-MM-DDTHH:mm:ssZ) (optional)")
    parser.add_argument("--stopping", help="the time to stop the search (YYYY-MM-DDTHH:mm:ssZ) (optional)")
    args = parser.parse_args()

    # configure a basic logger
    logging.basicConfig(format="%(asctime)s %(levelname)-8s - %(message)s", level=logging.INFO)

    try:
        main(**vars(args))
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.print_exc())
