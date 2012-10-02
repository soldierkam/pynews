# -*- coding: utf-8 *-*
import tweetstream
from tweetstream import ConnectionError
from lang import TwitterLangDetect
from my_collections import IncDict
import os
import sys
from gui import Gui
from threading import Thread, Event, Semaphore
from boilerpipe.extract import Extractor
from Queue import Queue, Empty
import shelve
import logging
from hashlib import sha1
from save import Manager as StreamMgr

ld = TwitterLangDetect()

TYPE_MEDIA = u"media"
TYPE_URL = u"url"

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, stream=sys.stdout)
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)

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
        self.__text = None
        self.__type = type

    def __eq__(self, other):
        if other:
            return self.__url == other.__url
        else:
            return False

    def __hash__(self):
        return 3 + 7 * self.__url.__hash__()

    def getText(self):
        return self.__text

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
        return u"{URL " + self.__type + u" :" + self.__url + u"}"

    def __str__(self):
        return self.__unicode__()

class NothingToDo(Exception):

    def __init(self):
        pass

class StoppableThread(Thread):

    def __init__(self, name):
        Thread.__init__(self)
        self.__name = name
        self.__stop = Event()

    def stop(self):
        self.__stop.set()

    def isStopping(self):
        return self.__stop.isSet()

    def run(self):
        self.atBegin()
        while not self.__stop.isSet():
            try:
                self.runPart()
            except NothingToDo:
                logger.info(self.__name + ": nothing to do")
                break
            except Exception as exc:
                logger.info(self.__name + ": error", exc)
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

    def atBegin(self):
        self.__shelve = shelve.open(self.__filename);
        logger.info("Load shelve from " + self.__filename + ": urls=" + unicode(len(self.__shelve)))

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
            elif type(msg) is GetMsg:
                if self.__shelve.has_key(msg.digest()):
                    msg.setResponse(self.__shelve.get(msg.digest()))
                else:
                    msg.setResponse(None)
            return
        except Empty:
            return

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
        while tryNumber < 3:
            try:
                extractor = Extractor(extractor='ArticleExtractor', url=url.getUrl())
                text = extractor.getText()
                url.setText(text)
                self.__mgr.addTextToStore(url)
                if tryNumber > 0:
                    logger.info(self.getThreadName() + ": finally resolved url " + unicode(url))
                break
            except Exception as e:
                logger.error(self.getThreadName() + ": cannot resolve url " + unicode(url))
                tryNumber += 1
        else:
            logger.error(self.getThreadName() + ": drop resolving url " + unicode(url))
        return

class UrlResolverManager():

    def __init__(self, filename):
        self.__queue = Queue()
        self.__workers = []
        self.__resolverCache = UrlResolverCache(filename)
        for i in range(0,3):
            self.__workers.append(UrlResolverWorker(self.__queue, self, i))

    def start(self):
        self.__resolverCache.start()
        for worker in self.__workers:
            worker.start()

    def stop(self):
        self.__resolverCache.stop()
        for worker in self.__workers:
            worker.stop()

    def addUrlToQueue(self, url):
        cachedValue = self.__resolverCache.get(url)
        if cachedValue:
            if url.getUrl() != cachedValue["url"]:
                raise Exception("Different url " + url.getUrl() + " != " +  cachedValue["url"])
            url.setText(cachedValue["text"])
            return
        self.__queue.put(url)
        s = self.__queue.qsize()
        if s % 20 == 0 and s > 20:
            logger.warning("Queue size is too big: " + unicode(s))

    def addTextToStore(self, url):
        self.__resolverCache.put(url)

class Model(StoppableThread):

    def __init__(self, gui, stream):
        StoppableThread.__init__(self, "Model")
        self.__stream= stream
        self.__url_counter = IncDict()
        self.__url_resolver = UrlResolverManager("/tmp/pynews.urls")
        self.__gui = gui;
        self.start()
        self.__url_resolver.start()
        self.__i = 0

    def runPart(self):
        for s in self.__stream:
            if self.isStopping():
                logger.info("Model end")
                return False
            if u'text' in s and s[u'retweet_count'] > 20:
                tweet = TweetText(s, self.__url_counter, self.__url_resolver)
                retweeted = TweetText(s["retweeted_status"], self.__url_counter, self.__url_resolver) if "retweeted_status" in s else None
                logger.info(unicode(tweet))
                logger.info(unicode(retweeted))
                if self.__i > 10:
                    print unicode(self.__url_counter)
                    self.__gui.updateUrls(self.__url_counter)
                    self.__i=0
                else:
                    self.__i+=1
        raise NothingToDo()

    def stop(self):
        StoppableThread.stop(self)
        self.__url_resolver.stop()

def main():
    logger.info("Start app")
    gui = Gui()
    mgr = StreamMgr()
    model = Model(gui, stream=mgr.restore("/tmp/tweets"))
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
