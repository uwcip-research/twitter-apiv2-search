import argparse
import gzip
import json
import logging
import os
import time
import traceback
from datetime import datetime
from logging.handlers import RotatingFileHandler

import tenacity
from tweepy import StreamingClient, StreamRule

# set logging
logging.captureWarnings(True)
LOGGER_NAME = "twitter-streamer"
logger = logging.getLogger(LOGGER_NAME)
log_path = "./logs/stream_logs.txt"
log_handler2 = RotatingFileHandler(log_path, maxBytes=200000, backupCount=5)
log_handler2.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s - %(message)s"))
logger.addHandler(log_handler2)
logger.setLevel(logging.INFO)  # TODO can change this te DEBUG


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


def parse_ref_tweet(tweet, users):
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

    author = users.get(tweet["author_id"])  # get user object
    # print('ref author is', author)
    if author:
        obj.update({
            "user_id": tweet["author_id"],
            "user_screen_name": author["username"],
            "user_name": author["name"],
            "user_description": author["description"],
            "user_location": author.get("location"),
            "user_created_at": author["created_at"],
            "user_followers_count": author["public_metrics"]["followers_count"],
            "user_friends_count": author["public_metrics"]["following_count"],
            "user_statuses_count": author["public_metrics"]["tweet_count"],
            "user_verified": author["verified"]
        })
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
                    ref_tweet_obj = parse_ref_tweet(includes_tweets[ref_tweet_dict['id']], users)
                    type = ref_tweet_dict['type']
                    obj['references_%s' % type] = ref_tweet_obj

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
                    "media_variants": media.get("variants"),
                    "media_alt_text": media.get("alt_text")
                }
                mobjs.append(mobj)
            obj['media_objects'] = mobjs

    if kwargs['includes_places']:
        includes_places = kwargs['includes_places']
        if (tweet.geo is not None) and ('place_id' in tweet.geo):
            place_id = tweet.geo['place_id']
            if place_id in includes_places:
                place = includes_places[place_id]
                pobj = {
                    "id": place.id,
                    'full_name': place.full_name,
                    "contained_within": place.contained_within,
                    "country": place.country,
                    "country_code": place.country_code,
                    'geo': place.geo,
                    "name": place.name,
                    "place_type": place.place_type
                }
                obj['place_object'] = pobj

    return obj


class TwitterStreamer(StreamingClient):
    def __init__(self, bearer_token, limit, output_dir, *args, **kwargs):
        super().__init__(bearer_token, *args, **kwargs)
        self.logger = logging.getLogger(LOGGER_NAME)

        self.lines = 0
        self.lines_per_file = 10000  # Replace this with the number of tweets you want to store per file

        # status monitoring
        self.start_time = time.time()
        self.total_count = 0

        # maximum number of tweets to get
        self.limit = limit

        ## Replace with appropriate file path followed by / where you want to store the file
        self.file_path = output_dir
        self.file_name = None
        self.file_object = None

    def _extract_one(self, tweet, users, **kwargs):
        return parse_tweet(tweet, users, **kwargs)

    def on_response(self, response):
        # step1: process includes
        includes = response.includes

        # extract users
        includes_users = {}
        if "users" in includes:
            ulist = includes['users']
            for user in ulist:
                includes_users[user.id] = user
        # print('includes_users', includes_users)

        # extract places
        includes_places = {}
        if 'places' in includes:
            plist = includes['places']
            for place in plist:
                includes_places[place.id] = place
        # print('includes_places', includes_places)

        includes_media = {}
        if 'media' in includes:
            for media in includes['media']:
                media_key = media['media_key']
                includes_media[media_key] = media

        # extract include tweets (these are the reference tweets)
        includes_tweets = {}
        if "tweets" in includes:
            tlist = includes['tweets']
            for tweet in tlist:
                includes_tweets[tweet.id] = tweet

        # step2: process actual tweets
        tweet = response.data
        logger.debug(f'tweet is {tweet}')
        jobj = self._extract_one(tweet, includes_users, includes_tweets=includes_tweets,
                                 includes_media=includes_media, includes_places=includes_places)
        self.save_data_self(jobj)

    @tenacity.retry(wait=tenacity.wait_fixed(1), stop=tenacity.stop_after_attempt(3))
    def save_data_self(self, item):
        self.lines = self.lines + 1
        self.total_count += 1
        try:
            if self.file_name is None or self.lines > self.lines_per_file:
                if self.file_object is not None:
                    self.file_object.close()
                    os.rename("{}.tmp.gz".format(self.file_name), "{}.gz".format(self.file_name))

                self.file_name = os.path.join(self.file_path,
                                              "data-{}.jsonl".format(datetime.now().strftime("%Y%m%d_%H%M%S.%f")))
                self.file_object = gzip.open("{}.tmp.gz".format(self.file_name), "at")
                self.lines = 1
                self.logger.info("switching files: {}.gz".format(self.file_name))

            logger.debug(f'item is {item}')
            print(json.dumps(item, default=str), file=self.file_object, flush=True)
            self.update_status()
        except Exception as e:
            self.logger.error("an error occurred while writing a line: {}".format(e))
            self.logger.error(traceback.format_exc())
            raise

    def on_errors(self, errors):
        for error in errors:
            self.logger.info("Twitter Streaming Error: {}".format(error))

    def update_status(self):
        if self.total_count >= self.limit:
            logger.info(f"collected a total of {self.total_count} tweets, exiting streamer...")
            self.on_exit()
            return

        if self.total_count % 1000 == 0:  # log update status
            hours_elapsed = (time.time() - self.start_time) / 3600.0
            logger.info(f"collected {self.total_count} tweets in {hours_elapsed} hours.")

    def on_exit(self):
        logger.debug("existing streamer")
        self.file_object.close()
        self.disconnect()


def get_bearer_token(file_name):
    with open(file_name, 'r') as f:
        credentials = json.load(f)
        return credentials['bearer_token']


def get_query(file_name):
    with open(file_name, 'r') as f:
        return json.load(f)


def run(credential_file, query_file, output_dir):
    bearer_token = get_bearer_token(credential_file)
    query = get_query(query_file)

    streamer = TwitterStreamer(bearer_token, query['limit'], output_dir, wait_on_rate_limit=True)

    # add rules
    rules = StreamRule(value=query['query'])
    streamer.add_rules(rules)

    # set fields
    user_fields = "created_at,description,entities,id,location,name,protected,public_metrics,url,username,verified,withheld"
    user_fields = query.get('user_fields', user_fields)

    tweet_fields = "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld,reply_settings"  # ,context_annotations
    tweet_fields = query.get('tweet_fields', tweet_fields)

    expansion_fields = "author_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id"
    expansion_fields = query.get("expansion_fields", expansion_fields)

    place_fields = None
    if query.get('include_place', False):
        place_fields = "contained_within,country,country_code,full_name,geo,id,name,place_type"
        place_fields = query.get('place_fields', place_fields)

    media_fields = None
    if query.get('include_media', False):
        media_fields = "media_key,type,url,duration_ms,height,width,public_metrics,alt_text,variants"
        media_fields = query.get('media_fields', media_fields)

    logger.info(f'user_fields: {user_fields}')
    logger.info(f'tweet_fields: {tweet_fields}')
    logger.info(f'expansion_fields: {expansion_fields}')
    logger.info(f'place_fields: {place_fields}')
    logger.info(f'media_fields: {media_fields}')

    # start data stream
    streamer.filter(user_fields=user_fields, tweet_fields=tweet_fields, place_fields=place_fields,
                    media_fields=media_fields, expansions=expansion_fields)

def main():
    # twitter v2 streamer
    parser = argparse.ArgumentParser(
        prog="stream",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )

    parser.add_argument("credentials", metavar="CREDENTIALS.JSON", help="json file containing twitter credentials")
    # see sample_query_file.json
    parser.add_argument("query", metavar="QUERY.TXT", help="file containing the stream query")
    parser.add_argument("output", help="output directory to store the output files")

    args = parser.parse_args()

    credentials_file = args.credentials
    query_file = args.query
    output = args.output
    if not os.path.exists(output):
        os.makedirs(output)

    print('args: credentials=%s, query_file=%s, output=%s' % (credentials_file, query_file, output))
    logger.info('args: credentials=%s, query_file=%s, output=%s' % (credentials_file, query_file, output))

    run(credentials_file, query_file, output)


if __name__ == "__main__":
    main()
