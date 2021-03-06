# -*- coding: utf-8 *-*
from Queue import Queue, Empty
import cookielib
from threading import Semaphore
import threading
from urllib2 import URLError, HTTPError, HTTPRedirectHandler, HTTPCookieProcessor
import urllib2
from urlparse import urlparse
from boilerpipe.extract import Extractor
import chardet
from inliner import Content
from lang import LangDetect
from logger import logger
import sqlite
from tools import StoppableThread, stringToDigest
from title import fetchTitle
from wx.lib.pubsub.pub import Publisher

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
        self.__digest = url.getUrlExpDigest()
        self.__mutex = Semaphore(0)

    def digest(self):
        return self.__digest

    def setResponse(self, res):
        self.__response = res
        self.__mutex.release()

    def getResponse(self):
        self.__mutex.acquire()
        return self.__response

    def getUrl(self):
        return self.__url

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
        self.__msgCount = 0

    def atBegin(self):
        self.__shelve = sqlite.open(self.__filename, protocol=-1, flags="w")
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
            if self.__msgCount % 60 == 0:
                logger.debug("Fetch msg... (qsize=" + str(self.__queue.qsize()) + ")")
                self.__shelve.sync()
            self.__msgCount += 1
            self.__msgCount %= 60
            msg = self.__queue.get(block=True, timeout=3)
            logger.debug(u"Msg: " + unicode(msg))
            if type(msg) is PutMsg:
                resolvedUrl = msg.getUrl()
                logger.debug(u"Put extracted url text in shelve: " + unicode(resolvedUrl))
                self.__shelve[resolvedUrl.getUrlExpDigest()] = {"text": None if resolvedUrl.isError() else resolvedUrl.getText(),
                                                             "htm": None if resolvedUrl.isError() else resolvedUrl.getHtml(),
                                                             "url": resolvedUrl.getExpandedUrl(),
                                                             "real_url": resolvedUrl.getRealUrl(),
                                                             "error": resolvedUrl.isError()}
                self.__size += 1
            elif type(msg) is GetMsg:
                cv = None
                has = self.__shelve.has_key(msg.digest())
                if has:
                    cv = self.__shelve.get(msg.digest())
                    if "htm" not in cv or "real_url" not in cv:
                        logger.info(u"Cached value do not contains data: " + unicode(cv))
                        del self.__shelve[msg.digest()]
                        cv = None
                    else:
                        if has and msg.getUrl() != cv["url"]:
                            logger.info(u"Fix url")
                            cv["url"] = msg.getUrl()
                            self.__shelve[msg.digest()] = cv
                        self.__hits += 1
                else:
                    logger.info(u"Cannot find url in cache: " + unicode(msg.getUrl()))
                msg.setResponse(cv)
                self.__requests += 1
            return
        except Empty:
            return

    def hitRate(self):
        return self.__hits * 100 / self.__requests if self.__requests != 0 else 0.0

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__shelve.close()

class UrlResolver():

    def __init__(self, url, mgr, cache):
        self.__url = url
        self.__mgr = mgr
        self.__cache = cache

    def isStopping(self):
        return False

    def __download(self, u):
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(HTTPRedirectHandler(), HTTPCookieProcessor(cj))
        opener.addheaders = [('User-agent', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)')]
        ct = opener.open(u)
        url = ct.geturl()
        if ct.headers.has_key("Content-Length"):
            size = int(ct.headers["Content-Length"])
            if size > 2 * 1024 * 1024:
                raise ValueError(u"Too big: " + url)
        contentType = ct.headers["content-type"] if ct.headers.has_key("content-type") else None
        content = Content(ct.read(), contentType)
        if content.mime() != "text/html":
            raise ValueError(u"Wrong mime: " + content.mime() + u"(" + url + u")")
        return content, url

    def resolve(self):
        tryNumber = 0
        cachable = True
        tryAgainLater = False
        logger.debug(u"Fetching " + unicode(self.__url))
        while tryNumber < 3 and not self.isStopping():
            try:
                #urlStr = url.getExpandedUrl() or url.getUrl()
                logger.debug(u"Try  " + unicode(tryNumber))
                content, url = self.__download(self.__url.getExpandedUrl())
                encoding = content.encoding() or chardet.detect(content.data())['encoding'] or "ISO-8859-1"
                data = unicode(content.data(), encoding)
                text = Extractor(extractor='ArticleExtractor', html=data).getText()
                if text is None:
                    raise ValueError("Extracted text is None")
                logger.debug(u"Set text " + unicode(self.__url))
                self.__url.setTextAndHtmlAndUrl(text, data, url)
                self.__cache.put(self.__url)
                self.__mgr.afterResolveUrl(self.__url)
                if tryNumber > 0:
                    logger.info("finally resolved url " + unicode(self.__url))
                break
            except HTTPError as err:
                logger.error("cannot resolve url " + str(self.__url) + " http error: " + str(err))
                tryNumber += 1
            except URLError as err:
                #DNS error
                cachable = False
                logger.error("2cannot resolve url " + str(self.__url) + " urlerror: " + str(err))
                tryNumber += 10
            except BaseException as err:
                logger.error("3cannot resolve url " + str(self.__url) + " exc: " + str(err))
                tryNumber += 1
        else:
            if cachable:
                logger.error("Drop resolving url " + unicode(self.__url))
                self.__url.setError()
                self.__cache.put(self.__url)
                self.__mgr.afterResolveUrl(self.__url)
            else:
                logger.error("Drop resolving url " + unicode(self.__url) + " - try again?")
                if not self.__url.isDnsError():
                    #jeżeli nie mamy ustawionego błędu to ustawiamy i wstawiamy do kolejki w celu weryfikacji
                    self.__url.setDnsError()
                    tryAgainLater = True
                    logger.error(u"Try again: " + unicode(self.__url))
                else:
                    self.__url.setError()
                    self.__mgr.afterResolveUrl(self.__url)
        return tryAgainLater

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
            logger.debug("Fetch url... (qsize=" + str(self.__queue.qsize()) + ")")
            url = self.__queue.get(True, 1)
            cachedValue = self.__cache.get(url)
            if cachedValue:
                if url.getExpandedUrl() != cachedValue["url"]:
                    raise Exception("Different url " + url.getExpandedUrl() + " != " +  cachedValue["url"])
                elif "error" in cachedValue and cachedValue["error"]:
                    url.setError()
                    self.__mgr.afterResolveUrl(url)
                    return
                else:
                    url.setTextAndHtmlAndUrl(cachedValue["text"], cachedValue["htm"], cachedValue["real_url"])
                    self.__mgr.afterResolveUrl(url)
                    return

            resolver = UrlStoppableResolver(url, self.__mgr, self.__cache, self)
            putInQueue = resolver.resolve()
            if putInQueue:
                logger.debug("Requeue url")
                if self.correctInternetConnection():
                    logger.error(u"Restart model")
                    Publisher.sendMessage("model.start", data={"soft": True})
                else:
                    logger.error(u"Pause model")
                    Publisher.sendMessage("model.pause", data={"soft": True})
                self.__queue.put(url)
        except Empty:
            return

    def correctInternetConnection(self):
        try:
            response=urllib2.urlopen('http://www.google.pl')
            return True
        except:
            return False

    def atEnd(self):
        StoppableThread.atEnd(self)
        logger.info("Queue size: " + str(self.__queue.qsize()))

class UrlResolverManager():

    def __init__(self, cacheFilename, tweetResolverListener):
        self.__queue = Queue(maxsize=50)
        self.__workers = []
        self.__tweetResolverListener = tweetResolverListener
        self.__resolverCache = UrlResolverCache(cacheFilename)
        for i in range(0,30):
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
        self.__queue.put(url, timeout=3)
        s = self.__queue.qsize()
        if s % 10 == 0 and s > 10 and self.__lastReportedQueueSize != s:
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
        tweet = url.tweet()
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
            if url.getExpandedUrl() != cachedValue["url"]:
                raise Exception("Different url " + url.getExpandedUrl() + " != " +  cachedValue["url"])
            elif "error" in cachedValue and cachedValue["error"]:
                url.setError()
                return
            else:
                url.setTextAndHtmlAndUrl(cachedValue["text"], cachedValue["htm"], cachedValue["real_url"])
                return
        resolver = UrlResolver(url, self, self.__resolverCache)
        if resolver.resolve():
            url.setError()

class UrlBuilder():

    def __init__(self):
        self.__mutex = threading.Semaphore()
        self.__deleted = set()

    def delete(self, url):
        try:
            logger.info(u"Delete url " + unicode(url))
            self.__mutex.acquire()
            self.__deleted.add(url.getUrlExpDigest())
            logger.debug(u"Delete url: done! (%s)" % unicode(len(self.__deleted)))
        finally:
            self.__mutex.release()

    def __call__(self, *args, **kwargs):
        try:
            self.__mutex.acquire()
            urlEntity = args[1]
            tweet = args[0]
            u = Url(urlEntity)
            key = u.getUrlDigest()
            if key in self.__deleted:
                raise UrlException(urlEntity, "Deleted")

            u.linkWithTweet(tweet)
            if not isinstance(u, Url):
                raise ValueError(u"Wrong object type " + unicode(u))
            return u
        finally:
            self.__mutex.release()

class Url:

    def __init__(self, entity):
        self.__state = None
        self.__url = entity["url"]
        self.__expanded = entity["expanded_url"]
        self.__realUrl = entity["expanded_url"]
        self.__validateUrl(self.__url, entity)
        self.__validateUrl(self.__expanded, entity)
        self.__text = None
        self.__title = None
        self.__html = None
        self.__error = False
        self.__dnsError = False
        self.__newsCategory = []
        self.__lang = None
        self.__tweet = None

    def linkWithTweet(self, tweet):
        self.__tweet = tweet
        tweet.addUrl(self)

    def getState(self):
        return self.__state

    def setState(self, state):
        if state not in ["pending", "finished"]:
            raise ValueError(state)
        self.__state = state

    def tweet(self):
        return self.__tweet

    def retweetsCount(self):
        return self.tweet().retweets()

    def mark(self):
        friendsSum = self.tweet().user().friendsCount()
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
        values["title"] = self.getTitle()
        values["tweet"] = self.__tweet.dump()
        return values

    def __eq__(self, other):
        if other and isinstance(other, Url):
            return self.__realUrl == other.__realUrl
        else:
            return False

    def __hash__(self):
        return 3 + 7 * self.__realUrl.__hash__()

    def getText(self):
        if self.__text is None:
            raise ValueError("Text is None: " + unicode(self.__realUrl))
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

    def setTextAndHtmlAndUrl(self, text, html, url):
        if text is None:
            raise ValueError("Text is None!")
        if html is None:
            raise ValueError("HTML is None!")
        if url is None:
            raise ValueError("URL is None!")
        logger.info(u"Url " + self.__realUrl + u" resolved")
        self.__text = text
        self.__html = html
        if self.__realUrl != url:
            logger.info(u"Redirected from \"" + self.__realUrl + u"\" to \"" + url + u"\"")
        self.__realUrl = url
        try:
            self.__lang = LangDetect.instance().detect(text) if text else None
        except BaseException as e:
            logger.exception(u"lang detect error: " + unicode(text))
            raise e

    def getHtml(self):
        return self.__html

    def getTitle(self):
        if self.__title is None and self.__html:
            self.__title = self._fetchTitle(self.__html)
        return self.__title

    def _fetchTitle(self, html):
        #tweetsText = " " * 200
        #for tweet in self.tweets():
        #    if len(tweetsText) > len(tweet.text()):
        #        tweetsText = tweet.text()

        titles = list()
        #titles.append((tweetsText.strip(), "t"))
        title = fetchTitle(html=html, titles=titles)#.strip().replace("\n", " ").replace("\t", " ")
        #while True:
        #    title2 = title.replace("  ", " ")
        #    if title2 == title:
        #        break
        #    title = title2
        return title

    def ignore(self):
        return self.isResolvedOrError() and self.__newsCategory is None

    def getUrlDigest(self):
        return stringToDigest(self.getUrl())

    def getUrlExpDigest(self):
        return stringToDigest(self.getExpandedUrl())

    def getRealUrlDigest(self):
        return stringToDigest(self.getRealUrl())

    def getUrl(self):
        return self.__url

    def getExpandedUrl(self):
        return self.__expanded

    def getRealUrl(self):
        return self.__realUrl

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
        return u"{URL: " + unicode(self.__realUrl) + u", cat:" + unicode(self.__newsCategory) + u", lang:" + unicode(self.__lang) + u", title:" + unicode(self.__title) + u"}"

    def __str__(self):
        return self.__unicode__()




class UrlException(Exception):

    def __init__(self, entity, msg="Invalid url in entity"):
        Exception.__init__(self)
        self.__entity = entity
        self.__msg = msg

    def __str__(self):
        return self.__msg + ": " + str(self.__entity)
