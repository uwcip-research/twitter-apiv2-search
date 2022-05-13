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


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def fetch(bearer_token, tweet_ids):
    try:
        params = {
            "ids": ",".join(tweet_ids),

            # "expand" every field possible. this takes id numbers that appear in a
            # tweet and turns them in actual readable text.
            "expansions": "author_id,referenced_tweets.id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id",

            # fill out these fields
            "user.fields": "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld",
            "tweet.fields": "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld",
            "place.fields": "contained_within,country,country_code,full_name,geo,id,name,place_type",
        }

        headers = {"Authorization": "Bearer {}".format(bearer_token)}
        r = requests.get("https://api.twitter.com/2/tweets", params=params, headers=headers)

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

        return r.json()
    except json.decoder.JSONDecodeError as e:
        logger.error(str(e))
        logger.error(traceback.format_exc())


def unwrap_references(tweet, linked_tweets):
    tweets = []
    for referenced_tweet in tweet.get("referenced_tweets", []):
        referenced_tweet_id = referenced_tweet["id"]
        if referenced_tweet_id in linked_tweets:
            tweets.append(linked_tweets[referenced_tweet_id])
            tweets.extend(unwrap_references(referenced_tweet, linked_tweets))

    return tweets


def parse(tweet_ids, raw, file_path):
    if "errors" in raw and isinstance(raw["errors"], list):
        for error in raw["errors"]:
            if error["resource_type"] == "tweet":
                # only record tweets that appeared in our search list
                tweet_id = error["resource_id"]
                if tweet_id not in tweet_ids:
                    continue

                with open(os.path.join(file_path, "{}.json".format(tweet_id)), "wt") as f:
                    print(json.dumps(error), file=f)

    users = {}  # keyed by user id
    for user in raw.get("includes", {}).get("users", []):
        user_id = user["id"]
        users[user_id] = user

    linked_tweets = {}  # keyed by tweet id
    for tweet in raw.get("includes", {}).get("tweets", []):
        tweet_id = tweet["id"]
        linked_tweets[tweet_id] = tweet

    groups = {}
    for tweet in raw.get("data", []):
        tweet_id = tweet["id"]
        if tweet_id not in tweet_ids:
            continue

        groups[tweet_id] = [tweet] + unwrap_references(tweet, linked_tweets)

    for group_id, tweets in groups.items():
        with open(os.path.join(file_path, "{}.json".format(group_id)), "wt") as f:
            for tweet in tweets:
                tweet_id = tweet["id"]
                author = users.get(tweet["author_id"])

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
                print(json.dumps(obj), file=f)

    return len(raw.get("data", []))


def main(**kwargs):
    # load credentials
    bearer_token = None
    with open("academic_credentials.json", "rt") as f:
        credentials = json.load(f)
        bearer_token = credentials.get("bearer_token")
        if bearer_token is None:
            raise RuntimeError("could not load bearer token from credentials.json")

    # see what we have loaded already
    loaded = [os.path.split(x)[1].split(".", -1)[0] for x in glob(os.path.join(kwargs["output"], "*")) if (x.endswith(".json"))]

    # get the list of ids
    tweet_ids = []
    for line in sys.stdin:
        tweet_id = line.strip().strip('"')

        # find tweet ids that we've already loaded and do not load them again
        if tweet_id not in loaded:
            tweet_ids.append(tweet_id)
        else:
            logger.info("already loaded {}".format(tweet_id))

    page = 0
    requested = 0
    found = 0
    for chunk in chunks(tweet_ids, 100):
        while True:
            results = fetch(bearer_token, chunk)
            if results is None:
                continue  # try the page again

            try:
                page = page + 1
                requested = requested + len(chunk)
                total = parse(chunk, results, kwargs["output"])
                found = found + total
                logger.info("page {} returned {} tweets, requested {} and fetched {} total tweets".format(page, total, requested, found))
                break
            except Exception as e:
                logger.error("GENERAL EXCEPTION: {}".format(e))
                logger.error(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="hydrate",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("--output", required=True, help="path to directory where outputs will be written")
    args = parser.parse_args()

    # configure a basic logger
    logging.basicConfig(format="%(asctime)s %(levelname)-8s - %(message)s", level=logging.INFO)

    try:
        main(**vars(args))
    except Exception as e:
        logger.error(str(e))
        logger.error(traceback.print_exc())
