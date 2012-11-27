# -*- coding: utf-8 *-*
import os
import shelve
from nltk import FreqDist
import pycountry
import tweepy
import twitter
from cache import CacheOp
from classifier import TxtClassificatorWrapper
from logger import logger
from news import tokenize
from oauth import OAuthClient
from tweet import TweetText
from url import UrlSyncResolverManager, UrlBuilder

class UserStore():

    def __init__(self, dir):
        self.__friendsCache = shelve.open(os.path.join(dir, "friends.db"), "c")
        self.__timelineCache = shelve.open(os.path.join(dir, "timeline.db"), "c")
        self.__homeTimelineCache = shelve.open(os.path.join(dir, "home_timeline.db"), "c")
        self.__auth = OAuthClient(os.environ["consumer_key"], os.environ["consumer_secret"])
        self.__baseUrl = "https://api.twitter.com/1.1"

    def getFriendsIds(self, userObj=None, userId=None):
        userId = userId or userObj.id()
        if userId is None:
            raise ValueError("Missing user id")
        return CacheOp(self.__friendsCache, userId, self.__getFriendsIds, userId=userId).get()

    def __getFriendsIds(self, userId):
        key, secret = self.__auth.accessToken()
        logger.info("Fetch friends ids for " + str(userId))
        api = twitter.Api(access_token_key=key, access_token_secret=secret)
        return api.GetFriendIDs(user=userId)

    def getTimeline(self, userObj=None, userId=None):
        userId = userId or userObj.id()
        if userId is None:
            raise ValueError("Missing user id")
        return CacheOp(self.__timelineCache, userId, self.__getTimeline, userId=userId).get()

    def getHomeTimeline(self, userObj=None, userId=None):
        userId = userId or userObj.id()
        if userId is None:
            raise ValueError("Missing user id")

        timeline = []
        max_id = None
        while True:
            timelinePart = CacheOp(self.__homeTimelineCache, userId, self.__getHomeTimeline, max_id=max_id).get()
            for elem in timelinePart:
                timeline.append(elem)
                max_id = elem["id"]
            if len(timelinePart) < 200:
                break


    def __getTimeline(self, user_id, include_rts=True, include_entities=True, count=200):
        key, secret = self.__auth.accessToken()
        logger.info("Fetch timeline for " + str(user_id))
        api = twitter.Api(access_token_key=key, access_token_secret=secret)
        #return api.GetUserTimeline(id=userId, include_entities=True, include_rts=True)
        parameters = {}

        url = '%s/statuses/user_timeline/%s.json' % (self.__baseUrl, user_id)

        if include_rts:
            parameters['include_rts'] = 1

        if include_entities:
            parameters['include_entities'] = 1
        parameters['count'] = count
        json = api._FetchUrl(url, parameters=parameters)
        return api._ParseAndCheckTwitter(json)

    def __getHomeTimeline(self, user_id, include_entities=True, count=200, max_id = None):
        key, secret = self.__auth.accessToken()
        logger.info("Fetch home timeline for " + str(user_id))
        api = twitter.Api(access_token_key=key, access_token_secret=secret)
        #return api.GetUserTimeline(id=userId, include_entities=True, include_rts=True)
        parameters = {}

        url = '%s/statuses/home_timeline.json' % self.__baseUrl

        if include_entities:
            parameters['include_entities'] = 1
        if max_id:
            parameters['max_id'] = max_id
        parameters['count'] = count
        json = api._FetchUrl(url, parameters=parameters)
        return api._ParseAndCheckTwitter(json)


class UserFeatures():

    __classifier = None

    def __init__(self, userStore, urlResolver, fd, userId):
        self.__store = userStore;
        self.__id = userId
        self.__urlResolver = urlResolver
        self.__fd = fd
        self.__classifier = None

    def getTimeline(self):
        return self.__store.getTimeline(userId=self.__id)

    def getFriendsIds(self):
        return self.__store.getFriendsIds(userId=self.__id)

    def getHomeTimeline(self):
        return self.__store.getHomeTimeline(userId=self.__id)

    def _classifier(self):
        if UserFeatures.__classifier is None:
            UserFeatures.__classifier = TxtClassificatorWrapper()
        return UserFeatures.__classifier

    def doJob(self):
        self.__urlBuilder = UrlBuilder(self.__fd)
        timeline = self.getTimeline()
        self.__getTimelineFeatures(timeline)
        friends = self.getFriendsIds()
        logger.info(u"Friends:" + ','.join(map(unicode, friends["ids"])))

    def __getTimelineFeatures(self, timeline):
        urls = []
        for t in timeline:
            tweet = TweetText(t, self.__urlBuilder)
            logger.debug(u"Tweet:" + unicode(tweet))
            for url in tweet.urls():
                self.__urlResolver.addUrlToQueue(url)
                urls.append(url)
            logger.info(u"Tweet:" + unicode(tweet))
        #Kategorie
        url2labels = {url.getExpandedUrl(): self._classifier().classify(url.getText()) for url in urls if not url.isError()}
        labelsFreq = FreqDist()
        for labels in url2labels.values():
            for label in labels:
                labelsFreq.inc(label)
        logger.info(u"Categories: "  + unicode(labelsFreq.items()))
        labelsFreqValues = [(item[0], item[1]) for item in labelsFreq.items() if item[0] not in ['short', 'medium', 'long']]
        #normalizacja
        labelsFreqValues = {label: float(freq) / float(max([f for l,f in labelsFreqValues])) for label, freq in labelsFreqValues}
        logger.info(u"Category factors: "  + unicode(labelsFreqValues))

        #Języki
        langFreq = FreqDist()
        for u in urls:
            langFreq.inc(u.lang())
        logger.info(u"Langs: " + unicode(langFreq.items()))

        #Kraje
        countryFreq = FreqDist()
        countries = [c.name.lower() for c in pycountry.countries]
        for url in urls:
            if not url.isError():
                text = tokenize(url.getText())
                for c in countries:
                    if c in text:
                        countryFreq.inc(c)
        logger.info(u"Countries: " + unicode(countryFreq.items()))
        #nazwy języków
        languagesFreq = FreqDist()
        languages = [c.name.lower() for c in pycountry.languages]
        for url in urls:
            if not url.isError():
                text = tokenize(url.getText())
                for l in languages:
                    if l in text:
                        languagesFreq.inc(l)
        logger.info(u"Languages: " + unicode(languagesFreq.items()))
        return labelsFreqValues

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
