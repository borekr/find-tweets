import ConfigParser
import tweepy
import argparse
import json
import MySQLdb


class config:

    def __init__(self, filename="find-tweets.cfg"):
        config = ConfigParser.SafeConfigParser()
        try:
            with open(filename) as f:
                config.readfp(f)
        except IOError:
            print "Error opening config file {}!".format(filename)
            exit()
        self.consumer_token = config.get("Twitter auth", "consumer_token")
        self.consumer_secret = config.get("Twitter auth", "consumer_secret")
        self.access_token = config.get("Twitter auth", "access_token")
        self.access_token_secret = config.get("Twitter auth", "access_token_secret")
        self.search_terms = [e.strip for e in config.get("Twitter auth", "search_terms").strip(",")]
        self.mysql_server = config.get("mysql", "server")
        self.mysql_port = config.get("mysql", "server_port")
        self.mysql_db_name = config.get("mysql", "db_name")


class SearchStream(tweepy.StreamListener):

    def on_data(self, data):
        data_dict = json.loads(data)
        print data.dict['blah']

    def on_error(self, status):
        if status == "420":
            print "420 error"
            return False


def TwitterSearch(config):
    listener = SearchStream()
    oauth = tweepy.OAuthHandler(config.consumer_token, config.consumer_secret)
    oauth.set_access_token = (config.access_token, config.access_token_secret)
    stream = tweepy.Stream(auth=auth, listener=listener)
    stream.filter(track=config.search_terms)


class MySQL:

    def __init__(self, config):


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Find tweets and dump them into a database")
    parser.add_argument("--config", nargs="?", default="find-tweets.cfg", help="config file to use (default is find_tweets.cfg)")
    args = parser.parse_args()
    config = config(args.config)
