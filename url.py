# -*- coding: utf-8 *-*
from Queue import Queue, Empty
import shelve
from threading import Semaphore
from urllib2 import URLError, HTTPError
import urllib2
from urlparse import urlparse
from boilerpipe.extract import Extractor
from lang import LangDetect
from logger import logger
from tools import StoppableThread, stringToDigest
from wx.lib.pubsub.pub import Publisher

ld = LangDetect()

class PutMsg(object):

    def __init__(self, url):
        self.__url = url

    def getUrl(self):
        return self.__url

    def __str__(self):
        return u"{Put:" + unicode(self.__url) + u"}"

class GetMsg(object):

    def __init__(self, url):
        self.__url = url.getExpandedUrl()
        self.__digest = url.getUrlDigest()
        self.__mutex = Semaphore(0)

    def digest(self):
        return self.__digest

    def setResponse(self, res):
        self.__response = res
        self.__mutex.release()

    def getResponse(self):
        self.__mutex.acquire()
        return self.__response

    def __str__(self):
        return u"{Get:" + unicode(self.__url) + u"}"


class UrlResolverCache(StoppableThread):

    def __init__(self, filename):
        StoppableThread.__init__(self)
        self.__filename= filename
        self.__queue = Queue()
        self.__hits = 0
        self.__requests = 0
        self.__onLoadSize = None
        self.__size = 0

    def atBegin(self):
        self.__shelve = shelve.open(self.__filename);
        self.__onLoadSize = len(self.__shelve)
        self.__size = self.__onLoadSize
        logger.info("Load shelve from " + self.__filename + ": urls=" + unicode(self.__onLoadSize))

    def get(self, url):
        msg = GetMsg(url)
        self.__queue.put(msg)
        return msg.getResponse()

    def put(self, url):
        self.__queue.put(PutMsg(url))

    def runPart(self):
        try:
            logger.debug("Fetch msg... (qsize=" + str(self.__queue.qsize()) + ")")
            msg = self.__queue.get(block=True, timeout=3)
            logger.debug("Msg: " + str(msg))
            if type(msg) is PutMsg:
                resolvedUrl = msg.getUrl()
                logger.debug("Put extracted url text in shelve: " + resolvedUrl.getUrl())
                self.__shelve[resolvedUrl.getUrlDigest()] = {"text": None if resolvedUrl.isError() else resolvedUrl.getText(), "url": resolvedUrl.getUrl()}
                self.__size += 1
            elif type(msg) is GetMsg:
                if self.__shelve.has_key(msg.digest()):
                    msg.setResponse(self.__shelve.get(msg.digest()))
                    self.__hits += 1
                else:
                    msg.setResponse(None)
                self.__requests += 1
            return
        except Empty:
            return

    def hitRate(self):
        return self.__hits * 100 / self.__requests if self.__requests != 0 else 0.0

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__shelve.sync()
        self.__shelve.close()

class UrlResolver():

    def __init__(self, url, mgr, cache):
        self.__url = url
        self.__mgr = mgr
        self.__cache = cache

    def isStopping(self):
        return False

    def resolve(self):
        tryNumber = 0
        cachable = True
        tryAgainLater = False
        logger.debug(u"Fetching " + unicode(self.__url))
        while tryNumber < 3 and not self.isStopping():
            try:
                #urlStr = url.getExpandedUrl() or url.getUrl()
                extractor = Extractor(extractor='ArticleExtractor', url=self.__url.getUrl())
                text = extractor.getText()
                if text is None:
                    raise ValueError("Extracted text is None")
                self.__url.setText(text)
                self.__cache.put(self.__url)
                self.__mgr.afterResolveUrl(self.__url)
                if tryNumber > 0:
                    logger.info("finally resolved url " + unicode(self.__url))
                break
            except HTTPError as err:
                logger.error("cannot resolve url " + str(self.__url) + ": " + str(err))
                tryNumber += 1
            except URLError as err:
                #DNS error
                cachable = False
                logger.error("cannot resolve url " + str(self.__url) + ": " + str(err))
                tryNumber += 10
            except BaseException as err:
                logger.error("cannot resolve url " + str(self.__url) + ": " + str(err))
                tryNumber += 1
        else:
            if cachable:
                logger.error("Drop resolving url " + unicode(self.__url))
                self.__url.setError()
                self.__cache.put(self.__url)
                self.__mgr.afterResolveUrl(self.__url)
            else:
                logger.error("Drop resolving url " + unicode(self.__url) + " - try again?")
                Publisher.sendMessage("model.pause")
                logger.error(u"Pause model")
                if not self.__url.isDnsError():
                    #jeżeli nie mamy ustawionego błędu to ustawiamy i wstawiamy do kolejki w celu weryfikacji
                    self.__url.setDnsError()
                    tryAgainLater = True
                    logger.error(u"Try again: " + unicode(self.__url))
                if self.correctInternetConnection():
                    logger.error(u"Restart model")
                    Publisher.sendMessage("model.start")
        return tryAgainLater

    def correctInternetConnection(self):
        try:
            response=urllib2.urlopen('http://www.google.pl',timeout=5)
            return True
        except:
            return False

class UrlStoppableResolver(UrlResolver):

    def __init__(self, url, mgr, cache, worker):
        UrlResolver.__init__(self, url, mgr, cache)
        self.__worker = worker

    def isStopping(self):
        return self.__worker.isStopping()

class UrlResolverWorker(StoppableThread):

    def __init__(self, mgr, queue, cache, id):
        StoppableThread.__init__(self, self.__class__.__name__ + str(id))
        self.__queue = queue
        self.__mgr = mgr
        self.__cache = cache

    def runPart(self):
        try:
            logger.debug("Fetch url...")
            url = self.__queue.get(True, 3)
            resolver = UrlStoppableResolver(url, self.__mgr, self.__cache, self)
            putInQueue = resolver.resolve()
            if putInQueue:
                self.__queue.put(url)
        except Empty:
            return

class UrlResolverManager():

    def __init__(self, cacheFilename, tweetResolverListener):
        self.__queue = Queue(maxsize=200)
        self.__workers = []
        self.__tweetResolverListener = tweetResolverListener
        self.__resolverCache = UrlResolverCache(cacheFilename)
        for i in range(0,3):
            self.__workers.append(UrlResolverWorker(self, self.__queue, self.__resolverCache, i))
        self.__lastReportedQueueSize = None

    def start(self):
        self.__resolverCache.start()
        for worker in self.__workers:
            worker.start()

    def stop(self):
        self.__resolverCache.stop()
        for worker in self.__workers:
            worker.stop()
        logger.info("Urls in queue: " + str(self.__queue.qsize()))
        while not self.__queue.empty():
            self.__queue.get_nowait()

    def pauseWorkers(self):
        for worker in self.__workers:
            worker.pauseJob()

    def continueWorkers(self):
        logger.debug("Continue workers job")
        for worker in self.__workers:
            worker.continueJob()

    def cacheHitRate(self):
        return self.__resolverCache.hitRate()

    def addUrlToQueue(self, url):
        if url.getState() is not None:
            logger.info(u"Url already in queue:" + unicode(url))
            return
        url.setState("pending")
        cachedValue = self.__resolverCache.get(url)
        if cachedValue:
            if url.getUrl() != cachedValue["url"]:
                raise Exception("Different url " + url.getUrl() + " != " +  cachedValue["url"])
            if cachedValue["text"]:
                url.setText(cachedValue["text"])
            else:
                url.setError()
            self.afterResolveUrl(url)
            return
        self.__queue.put(url, timeout=3)
        s = self.__queue.qsize()
        if s % 20 == 0 and s > 20 and self.__lastReportedQueueSize != s:
            self.__lastReportedQueueSize = s
            logger.warning("Queue size is too big: " + unicode(s))

    def afterResolveUrl(self, url):
        if url.getState() != "pending":
            raise ValueError(unicode(url))
        url.setState("finished")
        if not url.isError():
            url.getText()
        self.__notifyUrlResolved(url)

    def __notifyUrlResolved(self, url):
        for tweet in url.tweets():
            if tweet.isResolved():
                self.__tweetResolverListener.tweetResolved(tweet)

class UrlSyncResolverManager():

    def __init__(self, cacheFilename):
        self.__resolverCache = UrlResolverCache(cacheFilename)

    def start(self):
        self.__resolverCache.start()

    def stop(self):
        self.__resolverCache.stop()

    def pauseWorkers(self):
        pass

    def continueWorkers(self):
        pass

    #def cacheHitRate(self):
    #    return self.__resolverCache.hitRate()

    def afterResolveUrl(self, url):
        pass

    def addUrlToQueue(self, url):
        cachedValue = self.__resolverCache.get(url)
        if cachedValue:
            if url.getUrl() != cachedValue["url"]:
                raise Exception("Different url " + url.getUrl() + " != " +  cachedValue["url"])
            if cachedValue["text"]:
                url.setText(cachedValue["text"])
            else:
                url.setError()
            return
        resolver = UrlResolver(url, self, self.__resolverCache)
        resolver.resolve()

class UrlBuilder():

    def __init__(self, freqDist):
        self.__freqDist = freqDist
        self.__urls = {}

    def __call__(self, *args, **kwargs):
        urlEntity = args[1]
        tweet = args[0]
        u = Url(urlEntity)
        key = u.getUrlDigest()
        if self.__urls.has_key(key):
            u = self.__urls.get(key)
            u.linkWithTweet(tweet)
        else:
            self.__urls[key] = u
            u.linkWithTweet(tweet)
        if not isinstance(u, Url):
            raise ValueError(u"Wrong object type " + unicode(u))
        self.__freqDist.inc(u)
        return u

class Url:

    def __init__(self, entity):
        self.__state = None
        self.__url = entity["url"]
        self.__expanded = entity["expanded_url"]
        self.__validateUrl(self.__url, entity)
        self.__validateUrl(self.__expanded, entity)
        self.__text = None
        self.__error = False
        self.__dnsError = False
        self.__newsCategory = []
        self.__lang = None
        self.__tweets = []

    def linkWithTweet(self, tweet):
        self.__tweets.append(tweet)
        tweet.addUrl(self)

    def getState(self):
        return self.__state

    def setState(self, state):
        if state not in ["pending", "finished"]:
            raise ValueError(state)
        self.__state = state

    def tweets(self):
        return self.__tweets

    def retweetsCount(self):
        valuesInTime = [tweet.retweets() for tweet in self.tweets()]
        return max(valuesInTime) if len(valuesInTime) > 0 else 0

    def mark(self):
        friendsSum = sum([tweet.user().friendsCount() for tweet in self.tweets()])
        return self.retweetsCount() + friendsSum

    def __validateUrl(self, url, entity):
        parsed = urlparse(url) if url else None
        if not parsed:
            raise UrlException(entity, "Cannot parse")
        elif not parsed.scheme in ["http", "https"]:
            raise UrlException(entity, "Invalid scheme \"" + parsed.scheme + "\"")
        elif parsed.path == "/" and not parsed.query:
            raise UrlException(entity, "Url to main page")
        elif parsed.hostname == "t.co" and len(url) < 19:
            raise UrlException(entity, "Too short url")

    def dump(self):
        values = {}
        values["url"] = self.__expanded
        values["cat"] = self.__newsCategory
        values["text"] = self.__text
        values["lang"] = self.__lang
        values["tweets"] = [t.dump() for t in self.__tweets]
        return values

    def __eq__(self, other):
        if other and isinstance(other, Url):
            return self.__url == other.__url
        else:
            return False

    def __hash__(self):
        return 3 + 7 * self.__url.__hash__()

    def getText(self):
        if self.__text is None:
            raise ValueError("Text is None")
        return self.__text

    def setError(self):
        self.__text = None
        self.__error = True

    def setDnsError(self):
        self.__text = None
        self.__dnsError = True

    def isDnsError(self):
        return self.__dnsError

    def isError(self):
        return self.__error

    def setText(self, text):
        if text is None:
            raise ValueError("Text is None!")
        logger.info("Url " + self.__url + " resolved")
        self.__text = text
        self.__lang = ld.detect(text)

    def ignore(self):
        return self.isResolvedOrError() and self.__newsCategory is None

    def getUrlDigest(self):
        return stringToDigest(self.getUrl())

    def getUrl(self):
        return self.__url

    def getExpandedUrl(self):
        return self.__expanded

    def isResolved(self):
        return self.__text != None

    def isResolvedOrError(self):
        return self.isResolved() or self.isError()

    def lang(self):
        return self.__lang

    def isRoot(self):
        parsed = urlparse(self.__expanded) if self.__expanded else None
        if not parsed:
            raise UrlException(self.__expanded, "Cannot parse")
        return parsed.path in ["/", ""] and not parsed.query

    def documentClasses(self):
        return self.__newsCategory

    def setDocumentClasses(self, cat):
        self.__newsCategory = cat

    def __unicode__(self):
        return u"{URL: " + unicode(self.__expanded) + u", cat:" + unicode(self.__newsCategory) + u", lang:" + unicode(self.__lang) + u"}"

    def __str__(self):
        return self.__unicode__()




class UrlException(Exception):

    def __init__(self, entity, msg="Invalid url in entity"):
        Exception.__init__(self)
        self.__entity = entity
        self.__msg = msg

    def __str__(self):
        return self.__msg + ": " + str(self.__entity)
