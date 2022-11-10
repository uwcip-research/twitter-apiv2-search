import argparse
import json
import getpass
import os
import psycopg2
import re
import sys
import traceback
from glob import glob


# compile this for performance later in the module
NULL_TERMINATOR = re.compile(r"(?<!\\)\\u0000")


def replace_null_terminators(text: str, replacement: str = r""):
    return NULL_TERMINATOR.sub(replacement, text) if text is not None else None


def main(**kwargs):
    if os.path.isfile(kwargs["input"]):
        load(kwargs["host"], kwargs["database"], kwargs["username"], kwargs["table"], kwargs["input"])
    else:
        tweet_files = [x for x in glob(kwargs["input"]) if (x.endswith(".json") or x.endswith(".json.gz"))]
        for tweet_file in tweet_files:
            load(kwargs["host"], kwargs["database"], kwargs["username"], kwargs["table"], tweet_file)


def load(host: str, database: str, username: str, table: str, tweet_file: str):
    print("loading {} into {} on {} on {}".format(tweet_file, table, database, host))
    conn = psycopg2.connect(host=host, dbname=database, user=username)

    try:
        with open(tweet_file, "rt") as f:
            for index, line in enumerate(f):
                try:
                    line = replace_null_terminators(line)
                    data = json.loads(line)
                    if "id" not in data:
                        continue

                    data["source_files"] = [tweet_file]
                    with conn.cursor() as cur:
                        # convert the references to a json thingy
                        references = data.pop("references", None)
                        if references is not None:
                            data["linked"] = json.dumps(references)
                        else:
                            data["linked"] = None

                        cur.execute("""
                            INSERT INTO {table} (
                                id, conversation_id, created_at, tweet, hashtags, urls, source, language,
                                retweet_count, reply_count, like_count, quote_count, in_reply_to_user_id,
                                user_id, user_screen_name, user_name, user_description, user_location, user_created_at,
                                user_followers_count, user_friends_count, user_statuses_count, user_verified,
                                linked, source_files
                            ) VALUES (
                                %(id)s, %(conversation_id)s, %(created_at)s, %(tweet)s, %(hashtags)s, %(urls)s, %(source)s, %(language)s,
                                %(retweet_count)s, %(reply_count)s, %(like_count)s, %(quote_count)s, %(in_reply_to_user_id)s,
                                %(user_id)s, %(user_screen_name)s, %(user_name)s, %(user_description)s, %(user_location)s, %(user_created_at)s,
                                %(user_followers_count)s, %(user_friends_count)s, %(user_statuses_count)s, %(user_verified)s,
                                %(linked)s, %(source_files)s
                            )
                            ON CONFLICT (id) DO UPDATE SET
                                source_files = (SELECT ARRAY(SELECT DISTINCT UNNEST({table}.source_files || excluded.source_files)))
                        """.format(table=table), data)
                except Exception as e:
                    print("error on line {}".format(index))
                    raise

        conn.commit()
    except Exception:
        traceback.print_exc()
        try:
            self.cnx.rollback()
        except Exception:
            pass


if __name__ == "__main__":
    username = getpass.getuser()

    parser = argparse.ArgumentParser(
        prog="load_user_tweets",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", nargs="?", help="a directory full of files with tweets to load")
    parser.add_argument("--host", help="the database cluster to load the data into", default="venus.lab.cip.uw.edu")
    parser.add_argument("-d", "--database", help="the database to load the data into", default=username)
    parser.add_argument("-u", "--username", help="the name of the user to use when connecting to the database", default=username)
    parser.add_argument("-t", "--table", help="the name of the database table to load this into", required=True)
    args = parser.parse_args()

    try:
        main(**vars(args))
    except Exception:
        traceback.print_exc()
