import ConfigParser
import tweepy
import argparse
import json
import MySQLdb
import logging
import logging.config
import datetime


class Log:
    logs = logging.getLogger('find_tweets')


class Config:

    def __init__(self, filename="find-tweets.cfg", logger=None):
        config = ConfigParser.SafeConfigParser({'mysql_port': 3306})
        try:
            with open(filename) as f:
                config.readfp(f)
        except IOError:
            self.logger.critical("Error opening config file {}!".format(filename))
            exit()

        try:
            self.consumer_token = config.get("Twitter auth", "consumer_token")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing consumer_token in 'Twitter auth' section in config file!")
            exit()

        try:
            self.consumer_secret = config.get("Twitter auth", "consumer_secret")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing consumer_secret in 'Twitter auth' section in config file!")
            exit()

        try:
            self.access_token = config.get("Twitter auth", "access_token")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing access_token in 'Twitter auth' section in config file!")
            exit()

        try:
            self.access_token_secret = config.get("Twitter auth", "access_token_secret")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing access_token_secret in 'Twitter auth' section in config file!")
        try:
            self.search_terms = [e.strip for e in config.get("Twitter", "search_terms").strip(",")]
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing search_terms in 'Twitter' section in config file!")

        try:
            self.mysql_server = config.get("mysql", "server")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing server in 'mysql' section in config file!")

        self.mysql_port = config.get("mysql", "server_port")

        try:
            self.mysql_username = config.get("mysql", "username")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing username in 'mysql' section in config file!")

        try:
            self.mysql_password = config.get("mysql", "password")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing password in 'mysql' section in config file!")

        try:
            self.mysql_db_name = config.get("mysql", "db_name")
        except ConfigParser.NoOptionError:
            Log.logs.critical("Missing db_name in 'mysql' section in config file!")


class SearchStream(tweepy.StreamListener):

    def __init__(self, db):
        self.db = db
        super(SearchStream, self).__init__()

    def on_data(self, data):
        log.logs.debug(str(data))
        data_dict = json.loads(data)
        Log.logs.debug(str(data_dict))
        self.db.insert_tweet(data_dict["user"]["name"], data_dict["user"]["screen_name"], data_dict["id"]. data_dict["text"], data_dict["created_at"])

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
        try:
            self.db = mysql.connect(host=config.mysql_server, user=config.mysql_username, passwd=config.mysql_password, db=config.mysql_db_name, port=config.mysql_port)
        except MySQLdb.Error, e:
            log.logs.critical("MySQL: unable to open connection: {}".format(str(e)))

    def insert_tweet(self, username, screen_name, id, text, date):
        cursor = self.db.cursor()
        try:
            cursor.execute("""INSERT INTO tweets
                       (username, screen_name, id, text, date)
                       VALUES {1}, {2}, {3}, {4}, {5}""".format(username, screen_name, id, text, date))
        except MySQLdb.Error, e:
            log.logs.critical("MySQL: error inserting into the DB: {}".format(str(e)))

if __name__ == "__main__":

    Log.logs.info("Starting find_tweets")
    parser = argparse.ArgumentParser(description="Find tweets and dump them into a database")
    parser.add_argument("--config", nargs="?", default="find-tweets.cfg", help="config file to use (default is find_tweets.cfg)")
    parser.add_argument("--logconfig", nargs="?", default="logging.ini", help="config file for logging (default is logging.ini")
    args = parser.parse_args()
    try:
        logging.config.fileConfig(args.logconfig)
    except ConfigParser.NoSectionError:
        logging.basicConfig()
        log.logs.critical("Logging config file not found: {}".format(args.logconfig))
    Log.logs.debug(args)
    config = Config(args.config)
