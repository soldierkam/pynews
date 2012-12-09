# -*- coding: utf-8 *-*
import os
import shelve
from nltk import FreqDist
import pycountry
import tweepy
import twitter
from twitter import TwitterError
from cache import CacheOp
from classifier import TxtClassificatorWrapper
from logger import logger
from news import tokenize
from oauth import OAuthClient
from tweet import TweetText
from url import UrlSyncResolverManager, UrlBuilder
from user import UserBuilder

class UserStore():

    def __init__(self, dir):
        self.__friendsCache = shelve.open(os.path.join(dir, "friends.db"), "c")
        self.__timelineCache = shelve.open(os.path.join(dir, "timeline.db"), "c")
        self.__baseUrl = "https://api.twitter.com/1.1"

    def _accessToken(self):
        pass

    def getFriendsIds(self, token, userIdOrScreenName=None):
        if userIdOrScreenName is None:
            raise ValueError("Missing user id")
        return CacheOp(self.__friendsCache, userIdOrScreenName, self.__getFriendsIds, token=token, userIdOrScreenName=userIdOrScreenName).get()

    def __getFriendsIds(self, token, userIdOrScreenName):
        logger.info("Fetch friends ids for " + str(userIdOrScreenName))
        api = self.__api(token)
        return api.GetFriendIDs(user=userIdOrScreenName)

    def getTimeline(self, token, userIdOrScreenName=None):
        if userIdOrScreenName is None:
            raise ValueError("Missing user id")
        logger.info(u"Get timeline for " + unicode(userIdOrScreenName))
        timeline = CacheOp(self.__timelineCache, userIdOrScreenName, self.__getTimeline, token=token, userIdOrScreenName=userIdOrScreenName).get()
        if 'error' in timeline:
            raise TwitterError(timeline['error'])
        return timeline

    def __api(self, token):
        return twitter.Api(access_token_key=token.key, access_token_secret=token.secret, consumer_key=os.environ["consumer_key"], consumer_secret=os.environ["consumer_secret"])

    def __getTimeline(self, token, userIdOrScreenName, include_rts=True, include_entities=True, count=200):
        logger.info(u"Fetch timeline for " + unicode(userIdOrScreenName))
        api = self.__api(token)
        #return api.GetUserTimeline(id=userId, include_entities=True, include_rts=True)
        parameters = {}
        isDigit = unicode.isdigit(unicode(userIdOrScreenName))
        user_id = int(userIdOrScreenName) if isDigit else None
        screen_name = None if isDigit else userIdOrScreenName
        if user_id:
            url = '%s/statuses/user_timeline/%s.json' % (self.__baseUrl, user_id)
        elif screen_name:
            url = ('%s/statuses/user_timeline.json?screen_name=%s' % (self.__baseUrl, screen_name))

        if include_rts:
            parameters['include_rts'] = 1

        if include_entities:
            parameters['include_entities'] = 1
        parameters['count'] = count
        json = api._FetchUrl(url, parameters=parameters)
        return api._ParseAndCheckTwitter(json)


    def close(self):
        self.__friendsCache.close()
        self.__timelineCache.close()

PHASE={
    "GET_TIMELINE": 1,
    "GET_TIMELINE_URLS": 2,
    "GET_TIMELINE_FEATURES": 3,
    "GET_FRIENDS": 4
}

class UserFeatures():

    def __init__(self, userStore, urlResolver, fd, userId, screenName, token):
        self.__store = userStore;
        self.__id = userId
        self.__screenName = screenName
        self.__urlResolver = urlResolver
        self.__fd = fd
        self.__token = token
        self.__phase = 0
        self.__proc = 0.0
        self.__catFreq = None
        self.__langFreq = None
        self.__friends = None

    def getTimeline(self):
        u = self.__id or self.__screenName
        return self.__store.getTimeline(token=self.__token, userIdOrScreenName=u)

    def getFriendsIds(self):
        u = self.__id or self.__screenName
        return self.__store.getFriendsIds(token=self.__token, userIdOrScreenName=u)

    def screenName(self):
        return self.__screenName

    def _classifier(self):
        return TxtClassificatorWrapper.instance()

    def __changePhase(self, p):
        self.__phase = p
        self.__proc = 0

    def doJob(self):
        self.__urlBuilder = UrlBuilder(self.__fd)
        self.__userBuilder = UserBuilder()

        self.__changePhase(PHASE["GET_TIMELINE"])
        timeline = self.getTimeline()
        self.__proc = 100

        self.__getTimelineFeatures(timeline)

        self.__changePhase(PHASE["GET_FRIENDS"])
        friends = self.getFriendsIds()
        self.__proc = 100

        self.__friends = friends["ids"]
        logger.info(u"Friends:" + ','.join(map(unicode, self.__friends)))
        return self

    def __getTimelineFeatures(self, timeline):
        logger.info(u"Get timeline features")
        tweets = []
        self.__changePhase(PHASE["GET_TIMELINE_URLS"])
        for t in timeline:
            try:
                tweet = TweetText(t, self.__urlBuilder, self.__userBuilder)
            except:
                logger.exception(u"Error: \"" + unicode(t) + u"\"")
                raise ValueError(t)
            logger.debug(u"Tweet:" + unicode(tweet))
            tweets.append(tweet)

        urls = []
        ti = 0
        for tweet in tweets:
            for url in tweet.urls():
                self.__urlResolver.addUrlToQueue(url)
                urls.append(url)
            logger.info(u"Tweet:" + unicode(tweet))
            ti += 1
            self.__proc = 100 * float(ti) / float(len(tweets))

        #Kategorie
        self.__changePhase(PHASE["GET_TIMELINE_FEATURES"])
        url2labels = {}
        ui = 0
        for url in urls:
            if not url.isError():
                logger.debug(u"Classify" + unicode(url.getUrl()))
                url2labels[url.getExpandedUrl()] = self._classifier().classify(url.getText())
            ui += 1
            self.__proc = 100 * float(ui) / float(len(urls))

        labelsFreq = FreqDist()
        for labels in url2labels.values():
            for label in labels:
                labelsFreq.inc(label)
        self.__catFreq = labelsFreq.items()
        logger.info(u"Categories: "  + unicode(labelsFreq.items()))
        labelsFreqValues = [(item[0], item[1]) for item in labelsFreq.items() if item[0] not in ['short', 'medium', 'long']]
        #normalizacja
        labelsFreqValues = {label: float(freq) / float(max([f for l,f in labelsFreqValues])) for label, freq in labelsFreqValues}
        logger.info(u"Category factors: "  + unicode(labelsFreqValues))

        #Języki
        langFreq = FreqDist()
        for u in urls:
            langFreq.inc(u.lang())
        self.__langFreq = langFreq.items()
        logger.info(u"Langs: " + unicode(langFreq.items()))

        return labelsFreqValues

    def dump(self):
        values ={}
        values["langs"] = self.__langFreq
        values["cat"] = self.__catFreq
        values["friends"] = self.__friends
        values["phase"] = self.__phase
        values["proc"] = self.__proc
        return values

class UserMgr():

    def __init__(self, mainDir):
        self.__userStore = UserStore(mainDir)
        self.__urlResolverMgr = UrlSyncResolverManager(os.path.join(mainDir, "urlResolverCache.db"))
        self.__urlResolverMgr.start()
        self.__fd = FreqDist()

    def doJob(self, token, userId, screenName=None):
        return UserFeatures(self.__userStore, self.__urlResolverMgr, self.__fd, userId, screenName, token)

    def close(self):
        self.__urlResolverMgr.stop()
        self.__userStore.close()

class Token():

    def __init__(self, tup):
        self.key = tup[0]
        self.secret = tup[1]


if __name__ == "__main__":
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/user"
    mgr = UserMgr(mainDir)
    auth = OAuthClient(os.environ["consumer_key"], os.environ["consumer_secret"])
    token = Token(auth.accessToken())
    for userId in ["4iHD", "soldierkam"]:
        mgr.doJob(token, userId)
    mgr.close()
