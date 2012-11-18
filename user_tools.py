# -*- coding: utf-8 *-*
import os
import shelve
from nltk import FreqDist
import tweepy
import twitter
from cache import CacheOp
from logger import logger
from oauth import OAuthClient
from tweet import TweetText
from url import UrlSyncResolverManager, UrlBuilder

class UserStore():

    def __init__(self, dir):
        self.__friendsCache = shelve.open(os.path.join(dir, "friends.db"), "c")
        self.__timelineCache = shelve.open(os.path.join(dir, "timeline.db"), "c")
        self.__auth = OAuthClient(os.environ["consumer_key"], os.environ["consumer_secret"])

    def getFriendsIds(self, userObj=None, userId=None):
        userId = userId or userObj.id()
        if userId is None:
            raise ValueError("Missing user id")
        return CacheOp(self.__friendsCache, userId, self.__getFriendsIds).get()

    def __getFriendsIds(self, userId):
        key, secret = self.__auth.accessToken()
        logger.info("Fetch friends ids for " + str(userId))
        api = twitter.Api(access_token_key=key, access_token_secret=secret)
        return api.GetFriendIDs(user=userId)

    def getTimeline(self, userObj=None, userId=None):
        userId = userId or userObj.id()
        if userId is None:
            raise ValueError("Missing user id")
        return CacheOp(self.__timelineCache, userId, self.__getTimeline).get()

    def __getTimeline(self, user_id, include_rts=True, include_entities=True, count=200):
        key, secret = self.__auth.accessToken()
        logger.info("Fetch timeline for " + str(user_id))
        api = twitter.Api(access_token_key=key, access_token_secret=secret)
        #return api.GetUserTimeline(id=userId, include_entities=True, include_rts=True)
        parameters = {}

        url = '%s/statuses/user_timeline/%s.json' % (api.base_url, user_id)

        if include_rts:
            parameters['include_rts'] = 1

        if include_entities:
            parameters['include_entities'] = 1
        parameters['count'] = count
        json = api._FetchUrl(url, parameters=parameters)
        return api._ParseAndCheckTwitter(json)


class UserFeatures():

    def __init__(self, userStore, urlResolver, fd, userId):
        self.__store = userStore;
        self.__id = userId
        self.__urlResolver = urlResolver
        self.__fd = fd

    def getTimeline(self):
        return self.__store.getTimeline(userId=self.__id)

    def getFriendsIds(self):
        return self.__store.getFriendsIds(userId=self.__id)

    def doJob(self):
        timeline = self.getTimeline()
        urlBuilder = UrlBuilder(self.__urlResolver, self.__fd)
        for t in timeline:
            tweet = TweetText(t, urlBuilder)
            logger.info(u"Tweet:" + unicode(tweet))

        friends = self.getFriendsIds()
        logger.info(u"Friends:" + ','.join(map(unicode, friends["ids"])))

class UserMgr():

    def __init__(self, mainDir):
        self.__userStore = UserStore(mainDir)
        self.__urlResolverMgr = UrlSyncResolverManager(os.path.join(mainDir, "urlResolverCache.db"))
        self.__urlResolverMgr.start()
        self.__fd = FreqDist()

    def doJob(self, userId):
        userFeatures = UserFeatures(self.__userStore, self.__urlResolverMgr, self.__fd, userId)
        userFeatures.doJob()

    def close(self):
        self.__urlResolverMgr.stop()

if __name__ == "__main__":
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/user"
    mgr = UserMgr(mainDir)
    for userId in ["soldierkam", "4iHD"]:
        mgr.doJob(userId)
    mgr.close()
