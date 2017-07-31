import configparser
import tweepy
import argparse
import json
import pymysql.cursors
import logging
import logging.config
import datetime
import threading


class Log:
    logs = logging.getLogger('find_tweets')


class Config:

    def __init__(self, filename="find-tweets.cfg", logger=None):
        self.filename = filename
        config = configparser.SafeConfigParser({'mysql_port': 3306})
        try:
            with open(filename) as f:
                config.readfp(f)
        except IOError:
            self.logger.critical("Error opening config file {}!".format(filename))
            exit()

        try:
            self.consumer_token = config.get("Twitter auth", "consumer_token")
        except configparser.NoOptionError:
            Log.logs.critical("Missing consumer_token in 'Twitter auth' section in config file!")
            exit()

        try:
            self.consumer_secret = config.get("Twitter auth", "consumer_secret")
        except configparser.NoOptionError:
            Log.logs.critical("Missing consumer_secret in 'Twitter auth' section in config file!")
            exit()

        try:
            self.access_token = config.get("Twitter auth", "access_token")
        except configparser.NoOptionError:
            Log.logs.critical("Missing access_token in 'Twitter auth' section in config file!")
            exit()

        try:
            self.access_token_secret = config.get("Twitter auth", "access_token_secret")
        except configparser.NoOptionError:
            Log.logs.critical("Missing access_token_secret in 'Twitter auth' section in config file!")
        try:
            self.search_terms = [e.strip for e in config.get("Twitter", "search_terms").strip(",")]
        except configparser.NoOptionError:
            Log.logs.critical("Missing search_terms in 'Twitter' section in config file!")

        try:
            self.mysql_server = config.get("mysql", "server")
        except configparser.NoOptionError:
            Log.logs.critical("Missing server in 'mysql' section in config file!")

        self.mysql_port = config.get("mysql", "server_port")

        try:
            self.mysql_username = config.get("mysql", "username")
        except configparser.NoOptionError:
            Log.logs.critical("Missing username in 'mysql' section in config file!")

        try:
            self.mysql_password = config.get("mysql", "password")
        except configparser.NoOptionError:
            Log.logs.critical("Missing password in 'mysql' section in config file!")

        try:
            self.mysql_db_name = config.get("mysql", "db_name")
        except configparser.NoOptionError:
            Log.logs.critical("Missing db_name in 'mysql' section in config file!")

    def check_updates(self, config_change):
        # watch config file
        config_change.set()


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
            print("420 error")
            return False


def twitter_search(config, config_change):
    listener = SearchStream()
    oauth = tweepy.OAuthHandler(config.consumer_token, config.consumer_secret)
    oauth.set_access_token = (config.access_token, config.access_token_secret)
    stream = tweepy.Stream(auth=auth, listener=listener)
    Log.logs.info("Starting twitter stream")
    stream.filter(track=config.search_terms, async=True)
    while True:
        config_change.wait()
        Log.logs.info("Config change detected!")
        Log.logs.debug("New search terms: {}".format(config.search_terms))
        stream.disconnect()
        sleep(20)
        stream.filter(track=config.search_terms, async=True)


class MySQL:

    def __init__(self, config):
        try:
            self.connection = pymysql.connect(host=config.mysql_server, user=config.mysql_username, password=config.mysql_password, db=config.mysql_db_name, port=config.mysql_port, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        except pymysql.Error as e:
            log.logs.critical("MySQL: unable to open connection: {}".format(str(e)))

    def insert_tweet(self, username, screen_name, id, text, date):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""INSERT INTO `tweets`
                           (`username`, `screen_name`, `id`, `text`, `date`)
                           VALUES ({1}, {2}, {3}, {4}, {5})""".format(username, screen_name, id, text, date))
                connection.commit()
        except pymysql.Error as e:
            log.logs.critical("MySQL: error inserting into the DB: {}".format(str(e)))
        finally:
            self.connection.close()

if __name__ == "__main__":

    Log.logs.info("Starting find_tweets")
    parser = argparse.ArgumentParser(description="Find tweets and dump them into a database")
    parser.add_argument("--config", nargs="?", default="find-tweets.cfg", help="config file to use (default is find_tweets.cfg)")
    parser.add_argument("--logconfig", nargs="?", default="logging.ini", help="config file for logging (default is logging.ini")
    args = parser.parse_args()
    try:
        logging.config.fileConfig(args.logconfig)
    except configparser.NoSectionError:
        logging.basicConfig()
        log.logs.critical("Logging config file not found: {}".format(args.logconfig))
    Log.logs.debug(args)
    config = Config(args.config)
    config_change = threading.event
    config_thread = threading.Thread(target=config.check_updates, args=(config_change))
    config_thread.start()
    twitter_search(config, config_change)
