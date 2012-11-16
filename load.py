from logger import logger
import cPickle
from main import TweetText
from url import Url
from user import User

list = cPickle.load(open("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets/resolved_tweets.cpickle"))
for tweet in list:
    logger.info(unicode(tweet) + unicode(map(str, tweet.urls())) + unicode(tweet.user()))
