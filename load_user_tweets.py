import argparse
import json
import os
import psycopg2
import sys
import traceback
from glob import glob


def main(**kwargs):
    tweet_files = [x for x in glob(os.path.join(kwargs["input"], "*")) if (x.endswith(".json"))]
    for tweet_file in tweet_files:
        load(kwargs["host"], kwargs["database"], kwargs["table"], tweet_file)


def load(host: str, database: str, table: str, tweet_file: str):
    print("loading {} into {} on {} on {}".format(tweet_file, table, database, host))
    conn = psycopg2.connect(host=host, dbname=database, user="labuser")

    try:
        with open(tweet_file, "rt") as f:
            for index, line in enumerate(f):
                try:
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
    parser = argparse.ArgumentParser(
        prog="load_user_tweets",
        formatter_class=argparse.RawTextHelpFormatter,
        description=__doc__,
    )
    parser.add_argument("input", nargs="?", help="a directory full of files with tweets to load")
    parser.add_argument("--host", help="the database cluster to load the data into", required=True)
    parser.add_argument("-d", "--database", help="the database to load the data into", required=True)
    parser.add_argument("-t", "--table", help="the name of the database table to load this into", required=True)
    args = parser.parse_args()

    try:
        main(**vars(args))
    except Exception:
        traceback.print_exc()
