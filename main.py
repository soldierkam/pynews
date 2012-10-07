# -*- coding: utf-8 *-*
from httplib import HTTPException
import time
import urllib2
from urllib2 import URLError, HTTPError
from lang import TwitterLangDetect
from my_collections import IncDict
import os
from gui import Gui
from threading import Thread, Event, Semaphore
from boilerpipe.extract import Extractor
from Queue import Queue, Empty
import shelve
from hashlib import sha1
from save import Manager as StreamMgr
from urlparse import urlparse
from wx.lib.pubsub.pub import Publisher
from logger import logger
ld = TwitterLangDetect()

TYPE_MEDIA = u"media"
TYPE_URL = u"url"



class TweetText:

    def __init__(self, t, url_counter, url_resolver):
        self.__id = t["id"]
        self.__text = t["text"]
        self.__lang = ld.detect(t)
        self.__url = None
        self.__url_counter = url_counter
        self.__url_resolver = url_resolver
        self.__createUrl__(t['entities']['urls'], TYPE_URL)
        if 'media' in t['entities']:
            self.__createUrl__(t['entities']['media'], TYPE_MEDIA)

    def __createUrl__(self, entities, type):
        for urlEntity in entities:
            self.__url = Url(urlEntity, type)
            self.__url, urlExisted = self.__url_counter.inc(self.__url)
            if not urlExisted:
                self.__url_resolver.addUrlToQueue(self.__url)

    def __eq__(self, other):
        return self.__id == other.__id

    def __hash__(self):
        return 3 + 7 * self.__id.__hash__()

    def text(self):
        return self.__text

    def id(self):
        return self.__id

    def lang(self):
        return self.__text

    def __str__(self):
        return u":" + unicode(self.__text) + u"(" + unicode(self.__lang) + u"," + unicode(self.__id) + ")"

class Url:

    def __init__(self, entity, type):
        self.__url = entity["url"]
        self.__expanded = entity["expanded_url"]
        self.__validateUrl(self.__url, entity)
        self.__validateUrl(self.__expanded, entity)
        self.__text = None
        self.__type = type
        self.__dnsError = False

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
        return self.__text

    def setError(self):
        self.__text = "ERROR"

    def setDnsError(self):
        self.__dnsError = True

    def isDnsError(self):
        return self.__dnsError

    def setText(self, text):
        logger.info("Url " + self.__url + " resolved")
        self.__text = text

    def getUrlDigest(self):
        s = sha1()
        s.update(self.getUrl())
        return s.digest()

    def getUrl(self):
        return self.__url

    def getExpandedUrl(self):
        return self.__expanded

    def isResolved(self):
        return True if self.__text else False

    def __unicode__(self):
        return u"{URL " + self.__type + u" :" + self.__url + u", exp:" + unicode(self.__expanded) + u"}"

    def __str__(self):
        return self.__unicode__()

class NothingToDo(Exception):

    def __init(self):
        pass

class UrlException(Exception):

    def __init__(self, entity, msg="Invalid url in entity"):
        self.__entity = entity
        self.__msg = msg

    def __str__(self):
        return self.__msg + ": " + str(self.__entity)

class StoppableThread(Thread):

    def __init__(self, name):
        Thread.__init__(self, name=name)
        self.__name = name
        self.__stop = Event()
        self.__pauseEvent = Event()

    def stop(self):
        self.__stop.set()

    def isStopping(self):
        return self.__stop.isSet()

    def pauseJob(self):
        self.__pauseEvent.set()

    def continueJob(self):
        self.__pauseEvent.clear()

    def isPaused(self):
        return self.__pauseEvent.isSet()

    def run(self):
        self.atBegin()
        while not self.__stop.isSet():
            try:
                if self.__pauseEvent.isSet():
                    time.sleep(1)
                    continue
                self.runPart()
            except NothingToDo:
                logger.info(self.__name + ": nothing to do")
                break
            except BaseException as exc:
                logger.exception(self.__name + ": error")
                break
        self.atEnd()

    def getThreadName(self):
        return self.__name

    def runPart(self):
        pass

    def atBegin(self):
        pass

    def atEnd(self):
        logger.info(self.__name + ": exiting...")
        pass

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
                logger.debug("Put resolved url in shelve: " + resolvedUrl.getUrl())
                self.__shelve[resolvedUrl.getUrlDigest()] = {"text": resolvedUrl.getText(), "url":resolvedUrl.getUrl()}
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
        return self.__hits * 100 / self.__requests

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__shelve.sync()
        self.__shelve.close()

class UrlResolverWorker(StoppableThread):

    def __init__(self, queue, mgr, id):
        StoppableThread.__init__(self, "UrlResolverWorker" + str(id))
        self.__queue = queue
        self.__mgr = mgr

    def runPart(self):
        url = None
        while url == None:
            try:
                url = self.__queue.get(True, 3)
            except Empty:
                return

        tryNumber = 0
        cachable = True
        while tryNumber < 3 and not self.isStopping():
            try:
                urlStr = url.getExpandedUrl() or url.getUrl()
                extractor = Extractor(extractor='ArticleExtractor', url=url.getUrl())
                text = extractor.getText()
                url.setText(text)
                self.__mgr.addTextToStore(url)
                if tryNumber > 0:
                    logger.info(self.getThreadName() + ": finally resolved url " + unicode(url))
                break
            except HTTPError as err:
                logger.error(self.getThreadName() + ": cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 1
            except URLError as err:
                #DNS error
                cachable = False
                logger.error(self.getThreadName() + ": cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 10
            except BaseException as err:
                logger.error(self.getThreadName() + ": cannot resolve url " + str(url) + ": " + str(err))
                tryNumber += 1
        else:
            logger.error(self.getThreadName() + ": drop resolving url " + unicode(url))
            if cachable:
                url.setError()
                self.__mgr.setError(url)
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

    def __init__(self, filename):
        self.__queue = Queue(maxsize=200)
        self.__workers = []
        self.__resolverCache = UrlResolverCache(filename)
        for i in range(0,3):
            self.__workers.append(UrlResolverWorker(self.__queue, self, i))
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
            url.setText(cachedValue["text"])
            return
        self.__queue.put(url, timeout=60*5)
        s = self.__queue.qsize()
        if s % 20 == 0 and s > 20 and self.__lastReportedQueueSize != s:
            self.__lastReportedQueueSize = s
            logger.warning("Queue size is too big: " + unicode(s))

    def addTextToStore(self, url):
        self.__resolverCache.put(url)

    def setError(self, url):
        self.__resolverCache.put(url)

class Model(StoppableThread):

    def __init__(self, gui, stream, cacheDir):
        StoppableThread.__init__(self, "Model")
        self.__stream= stream
        self.__url_counter = IncDict()
        self.__url_resolver = UrlResolverManager(cacheDir)
        self.__gui = gui;
        self.__url_resolver.start()
        self.__refreshGui = Event()
        Publisher.subscribe(self.pauseJob, "model.pause")
        Publisher.subscribe(self.continueJob, "model.start")
        Publisher.subscribe(self.onRefreshGui, "model.refreshGui")
        self.doPauseJob()
        self.start()

    def onRefreshGui(self, msg):
        self.__refreshGui.set()

    def pauseJob(self, msg):
        self.doPauseJob()

    def doPauseJob(self):
        StoppableThread.pauseJob(self)
        self.__url_resolver.pauseWorkers()
        Publisher.sendMessage("model.paused")

    def continueJob(self, msg):
        self.doContinueJob()

    def doContinueJob(self):
        StoppableThread.continueJob(self)
        self.__url_resolver.continueWorkers()
        Publisher.sendMessage("model.started")

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
                    tweet = TweetText(s, self.__url_counter, self.__url_resolver)
                    retweeted = TweetText(s["retweeted_status"], self.__url_counter, self.__url_resolver) if "retweeted_status" in s else None
                except UrlException as e:
                    logger.exception("Cannot build url")
                    continue
                logger.info(unicode(tweet))
                logger.info(unicode(retweeted))
                if self.__refreshGui.isSet():
                    self.__refreshGui.clear()
                    data = {}
                    data["urls"] = self.__url_counter
                    data["cache"] = self.__url_resolver.cacheHitRate()
                    data["position"] = iter.position()
                    data["position_end"] = iter.count()
                    data["current_file_c"] = iter.currentFile()
                    data["last_file_c"] = iter.filesCount()
                    Publisher.sendMessage("update.urls", data=data)
        raise NothingToDo()

    def stop(self):
        StoppableThread.stop(self)
        self.__url_resolver.stop()

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets"
    logger.info("Start app")
    gui = Gui()
    mgr = StreamMgr()
    model = Model(gui, stream=mgr.restore(mainDir), cacheDir=os.path.join(mainDir, "cache"))
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
