# twitter apiv2 search

This includes a few different scripts. These aren't perfect and you may need to
tweak them to meet your needs.

## Getting Started

Before you can use any of these scripts you need to create a file with your
Twitter APIv2 credentials. It should look like this:

```jsonfile
{
    "bearer_token": "lasdfjkla"
}
```

Put your bearer token in there and then lock the file by running this command:

```
chmod 600 academic_credentials.json
```

The credentials must be named `academic_credentials.json` (or you can edit the
Python scripts to change the name of the credentials file.)

## Output

By default, the output of these programs includes the tweets that match your
search plus the tweets that those tweets reference. For example, if your search
matches a tweet that retweeted another tweet then both your tweet and the
retweeted tweet will appear in the results. This is why you can end up with a
lot of tweets from accounts that you do not recognize.

## Caveats

This will not return tweets that have been deleted or where the account has
been deleted or suspended.

## counts.py

This will return the number of times per minute, hour, or day that a search
pattern appears. You can see the [Twitter APIv2 documentation](https://developer.twitter.com/en/docs/twitter-api/tweets/search/introduction) or look at [this presentation](https://docs.google.com/presentation/d/13BMR4N5xlYLR6HRyjOJ6UJgG3KH6jV828uARnZK8EJQ/edit?usp=sharing) that covers the gist of it.

This example shows how many tweets per day the user `TwitterDev` has made.

```
python3 counts.py \
  --starting="2021-01-01T00:00:00Z" \
  --stopping="2021-02-11T00:00:00Z" \
  "from:TwitterDev"
```

## search.py

This will search for tweets using a search pattern. You can see the [Twitter APIv2 documentation](https://developer.twitter.com/en/docs/twitter-api/tweets/search/introduction) or look at [this presentation](https://uwnetid.sharepoint.com/:p:/r/sites/ischoolusers/CIP/Infrastructure/Training/Twitter%20APIv2.pptx?d=w0ba1ce4d948a475791806a67295d34fc&csf=1&web=1&e=DcE8aI) that covers the gist of it.

This example searches for tweets from the user `TwitterDev` between the given
dates. It will output the tweets and all tweets that they reference to a file
called `TwitterDev.json` in the same directory where the command is run.

```
python3 search.py \
  --starting="2021-01-01T00:00:00Z" \
  --stopping="2021-02-11T00:00:00Z" \
  "from:TwitterDev" > TwitterDev.json
```

## fetch_user_tweets.py

This will fetch all user tweets when given a list of users. The list can
contain either user names or user IDs, one per line. Nothing else can be on the
line. For example:

```
TwitterDev
Jack
katestarbird
1362916136254201860
```

The results will be written to the given file path. Each user name or ID will
be written to one file. So in the example above the output directory would
contain four files called `TwitterDev.json`, `Jack.json`, etc.

This example will create a directory for your search results and will fetch
all tweets from the users in the file called `myinputfile.txt` and put them
into the directory that you just created.

```
mkdir searchoutput
cat myinputfile.txt | python3 fetch_user_tweets.py --output ./searchoutput
```

## load_user_tweets.py

This will load all of the fetched tweets into a database table. First you need
to create the table. Look at the `load_user_tweets.sql` file to see what the
table should look like. You can create it in your database on venus. You might
want to create different tables with different names so change the name.

This example will load all of the tweets in the `searchoutput` directory into
the table called `foobar` in your database on venus.

```
python3 load_user_tweets.py ./searchoutput --host=venus.lab.cip.uw.edu --table=foobar
```
