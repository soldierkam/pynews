# -*- coding: utf-8 *-*
from urllib2 import URLError, HTTPError
from nltk import FreqDist
from clustering import DocumentSizeClustering
from lang import TwitterLangDetect, LangDetect
from gui import Gui
from threading import Event, Semaphore
from boilerpipe.extract import Extractor
from Queue import Queue, Empty
from news import NewsClassificator
from save import Manager as StreamMgr
from urlparse import urlparse
from wx.lib.pubsub.pub import Publisher
from logger import logger
from tools import StoppableThread, NothingToDo, stringToDigest
from user import User
import shelve, os,cPickle, time, urllib2

tld = TwitterLangDetect()
ld = LangDetect()

TYPE_MEDIA = u"media"
TYPE_URL = u"url"

class UrlBuilder():

    def __init__(self, urlResolver, freqDist):
        self.__urlResolver = urlResolver
        self.__freqDist = freqDist
        self.__urls = {}

    def init(self):
        self.__urlResolver.start()

    def pauseResolver(self):
        self.__urlResolver.pauseWorkers()

    def resumeResolver(self):
        self.__urlResolver.continueWorkers()

    def stop(self):
        self.__urlResolver.stop()

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
            self.__urlResolver.addUrlToQueue(u)
        self.__freqDist.inc(u)
        return u

class TweetText:

    def __init__(self, t, urlBuilder):
        self.__id = t["id"]
        self.__text = t["text"]
        #self.__lang = tld.detect(t)
        self.__urls = []
        self.__user = User(t["user"])
        self.__createUrl__(urlBuilder, t['entities']['urls'])
        if 'media' in t['entities']:
            self.__createUrl__(urlBuilder, t['entities']['media'])

    def __createUrl__(self, urlBuilder, entities):
        for urlEntity in entities:
            urlBuilder(self, urlEntity)

    def addUrl(self, url):
        self.__urls.append(url)

    def urls(self):
        return self.__urls

    def __eq__(self, other):
        return self.__id == other.__id

    def __hash__(self):
        return 3 + 7 * self.__id.__hash__()

    def text(self):
        return self.__text

    def user(self):
        return self.__user

    def id(self):
        return self.__id

    #def lang(self):
    #    return self.__lang

    def isResolved(self):
        tweetResolved = True
        for url in self.urls():
            if not url.isResolvedOrError():
                tweetResolved = False
        return tweetResolved

    def __str__(self):
        return u":" + unicode(self.__text) + u"(" + unicode(self.__id) + ")"

class TxtClassificatorWrapper():

    def __init__(self):
        self.__documentSizeClassificator = DocumentSizeClustering("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/datasets/extractedText.db")
        self.__newsClassificator = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/datasets/googlenews/", doTest=False)
        #pass

    def classify(self, txt):
        return self.__documentSizeClassificator.classify(txt), self.__newsClassificator.classify(txt)

class Url:

    def __init__(self, entity):
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

    def tweets(self):
        return self.__tweets

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

    def __eq__(self, other):
        if other:
            return self.__url == other.__url
        else:
            return False

    def __hash__(self):
        return 3 + 7 * self.__url.__hash__()

    def getText(self):
        if self.__text is None:
            raise ValueError("Text in None")
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
        elif parsed.path in ["/", ""] and not parsed.query:
            return True
        return False

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

class PutMsg(object):

    def __init__(self, url):
        self.__url = url

    def getUrl(self):
        return self.__url

class GetMsg(object):

    def __init__(self, url):
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

class UrlResolverCache(StoppableThread):

    def __init__(self, filename):
        StoppableThread.__init__(self, "UrlResolverCache")
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
            msg = self.__queue.get(block=True, timeout=3)
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

class UrlResolverWorker(StoppableThread):

    def __init__(self, mgr, queue, cache, id):
        StoppableThread.__init__(self, self.__class__.__name__ + str(id))
        self.__queue = queue
        self.__mgr = mgr
        self.__cache = cache

    def runPart(self):
        url = None
        while url is None:
            try:
                url = self.__queue.get(True, 3)
            except Empty:
                return

        tryNumber = 0
        cachable = True
        while tryNumber < 3 and not self.isStopping():
            try:
                #urlStr = url.getExpandedUrl() or url.getUrl()
                extractor = Extractor(extractor='ArticleExtractor', url=url.getUrl())
                text = extractor.getText()
                if text is None:
                    raise ValueError("Extracted text is None")
                url.setText(text)
                self.__cache.put(url)
                self.__mgr.afterResolveUrl(url)
                if tryNumber > 0:
                    logger.info("finally resolved url " + unicode(url))
                break
            except HTTPError as err:
                logger.error("cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 1
            except URLError as err:
                #DNS error
                cachable = False
                logger.error("cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 10
            except BaseException as err:
                logger.error("cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 1
        else:
            logger.error("drop resolving url " + unicode(url))
            if cachable:
                url.setError()
                self.__cache.put(url)
                self.__mgr.afterResolveUrl(url)
            else:
                Publisher.sendMessage("model.pause")
                if not url.isDnsError():
                    #jeżeli nie mamy ustawionego błędu to ustawiamy i wstawiamy do kolejki w celu weryfikacji
                    url.setDnsError()
                    self.__queue.put(url)
                if self.correctInternetConnection():
                    Publisher.sendMessage("model.start")

        return

    def correctInternetConnection(self):
        try:
            response=urllib2.urlopen('http://www.google.pl',timeout=5)
            return True
        except:
            return False

class UrlResolverManager():

    def __init__(self, filename, tweetResolverListener):
        self.__queue = Queue(maxsize=200)
        self.__workers = []
        self.__tweetResolverListener = tweetResolverListener
        self.__resolverCache = UrlResolverCache(filename)
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
        for worker in self.__workers:
            worker.continueJob()

    def cacheHitRate(self):
        return self.__resolverCache.hitRate()

    def addUrlToQueue(self, url):
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
        self.__queue.put(url)
        s = self.__queue.qsize()
        if s % 20 == 0 and s > 20 and self.__lastReportedQueueSize != s:
            self.__lastReportedQueueSize = s
            logger.warning("Queue size is too big: " + unicode(s))

    def afterResolveUrl(self, url):
        self.__notifyUrlResolved(url)

    def __notifyUrlResolved(self, url):
        for tweet in url.tweets():
            if tweet.isResolved():
                self.__tweetResolverListener.tweetResolved(tweet)

class ResolvedTweetQueue(StoppableThread):

    def __init__(self, dir, classificator):
        StoppableThread.__init__(self, self.__class__.__name__)
        self.__queue = Queue()
        self.__classificator = classificator
        self.__tweets = []
        self.__dir = dir
        self.start()

    def tweetResolved(self, tweet):
        self.__queue.put(tweet)

    def runPart(self):
        try:
            tweet = self.__queue.get(block=True, timeout=3)
            url = tweet.urls()[0]
            if url.isError():
                logger.info(u"Tweet bad: wrong url: " + unicode(tweet) + u" " + unicode(url))
                return
            url.setDocumentClasses(self.__classificator.classify(url.getText()))
            if url.isRoot() or url.lang() != "en" or "short" in url.documentClasses():
                logger.info(u"Tweet bad: " + unicode(tweet) + u" " + unicode(url))
                return
            logger.info(u"Tweet good: " + unicode(tweet) + u" " + unicode(url))
            logger.info(u"URL: " + unicode(url))
            self.__tweets.append(tweet)
            if len(self.__tweets) % 100 == 99:
                self.__store()
            return
        except Empty:
            return

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__store()

    def __tweetWithUrlToRoot(self, tweet):
        for u in tweet.urls():
            if u.isRoot():
                return True
        return False

    def __store(self):
        file = os.path.join(self.__dir, "resolved_tweets.cpickle")
        os.remove(file)
        outputFile = open(file, "w")
        cPickle.dump(self.__tweets, outputFile)
        outputFile.close()

class Model(StoppableThread):

    def __init__(self, gui, stream, cacheDir):
        StoppableThread.__init__(self, "Model")
        self.__stream= stream
        self.__gui = gui;
        self.__urlFreq = FreqDist()
        self.__tweetResolvedListener = ResolvedTweetQueue(cacheDir, TxtClassificatorWrapper())
        self.__urlResolver = UrlResolverManager(os.path.join(cacheDir, "cache"), self.__tweetResolvedListener)
        self.__urlBuilder = UrlBuilder(self.__urlResolver, self.__urlFreq)
        self.__refreshGui = Event()
        Publisher.subscribe(self.onPauseJob, "model.pause")
        Publisher.subscribe(self.onResumeJob, "model.start")
        Publisher.subscribe(self.onRefreshGui, "model.refreshGui")
        self.doPauseJob()
        self.start()

    def onRefreshGui(self, msg):
        self.__refreshGui.set()

    def onPauseJob(self, msg):
        self.doPauseJob()

    def doPauseJob(self):
        StoppableThread.pauseJob(self)
        self.__urlBuilder.pauseResolver()
        Publisher.sendMessage("model.paused")

    def onResumeJob(self, msg):
        self.doContinueJob()

    def doContinueJob(self):
        StoppableThread.continueJob(self)
        self.__urlBuilder.resumeResolver()
        Publisher.sendMessage("model.started")

    def atBegin(self):
        logger.info("Preparing model...")
        self.__urlBuilder.init()
        logger.info("Start analyzing tweets")

    def runPart(self):
        iter = self.__stream.__iter__()
        for s in iter:
            if self.isStopping():
                logger.info("Model end")
                return False
            if self.isPaused():
                time.sleep(1)
                return

            if u'text' in s and s[u'retweet_count'] > 20:
                try:
                    tweet = TweetText(s, self.__urlBuilder)
                    retweeted = TweetText(s["retweeted_status"], self.__urlBuilder) if "retweeted_status" in s else None
                except UrlException as e:
                    logger.warn("Cannot build url: " + str(e))
                    continue
                #logger.info(unicode(tweet))
                #logger.info(unicode(retweeted))
                if self.__refreshGui.isSet():
                    self.__refreshGui.clear()
                    data = {}
                    data["urls"] = {sample: self.__urlFreq.freq(sample) for sample in set(self.__urlFreq.samples())}
                    data["cache"] = self.__urlResolver.cacheHitRate()
                    data["position"] = iter.position()
                    data["position_end"] = iter.count()
                    data["current_file_c"] = iter.currentFile()
                    data["last_file_c"] = iter.filesCount()
                    Publisher.sendMessage("update.urls", data=data)
        raise NothingToDo()

    def stop(self):
        StoppableThread.stop(self)
        self.__urlBuilder.stop()
        self.__tweetResolvedListener.stop()

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets"
    logger.info("Start app")
    gui = Gui()
    mgr = StreamMgr()
    model = Model(gui, stream=mgr.restore(mainDir), cacheDir=mainDir)
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
