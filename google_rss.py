import Queue
from Queue import Empty
from _socket import timeout
import codecs
from datetime import time
import logging
import os
import urlparse
from inliner import REMOVE
from tools import stringToDigest
import random
import threading
import time
import feedparser
from boilerpipe.extract import Extractor
from inliner import Downloader as PageDownloader
from urllib2 import urlopen
from tools import StoppableThread
from logger import logger

__author__ = 'soldier'


#w - specifies the world topic
WORLD="w"
#b - specifies the business topic
BUSINESS="n"
#n - specifies the nation topic
NATION="n"
#t - specifies the science and technology topic
#SCI_TECH="t"
TECHNOLOGY = "tc"
SCIENCE = "snc"
#el - specifies the elections topic
ELECTIONS="el"
#p - specifies the politics topic
POLITICS="p"
#e - specifies the entertainment topic
ENTERTAINMENT="e"
#s - specifies the sports topic
SPORT="s"
#m - specifies the health topic
HEALTH="m"

#h - specifies the top headlines topic
HEADLINES="h"
SPOTLIGHT = "ir"

class GoogleNewsUrl():

    def __init__(self):
        self.__topic = None
        self.__edition = None

    def setTopic(self, t):
        self.__topic = t
        return self

    def setEdition(self, e):
        self.__edition = e
        return self

    def build(self):
        return "http://news.google.com/news?output=rss&ned=" + self.__edition + "&topic=" + self.__topic

class RssAnalyzer():

    def __init__(self, dir):
        self.__downloader = UrlDownloaderController(dir)
        self.__pollers = []
        for lang in ["us", "uk", "au", "en_ca"]:
            for topic in [HEADLINES, WORLD, NATION, SCIENCE, TECHNOLOGY, ELECTIONS, POLITICS, ENTERTAINMENT, SPORT, HEALTH, BUSINESS]:
                url = GoogleNewsUrl().setEdition(lang).setTopic(topic).build()
                p = RssPoller(url, topic, lang, self)
                self.__pollers.append(p)
                p.start()

    def addToQueue(self, url, topic):
        return self.__downloader.addToQueue(url, topic)

INTERVAL = 60

class RssPoller(StoppableThread):

    def __init__(self, newsUrl, topic, lang, rssAnalyzer):
        StoppableThread.__init__(self, "Rss-" + topic + "-" + lang)
        self.__url = newsUrl
        self.__lang = lang
        self.__topic = topic
        self.__rssAnalyzer = rssAnalyzer
        self.__secondsSinceLastCheck = random.randint(0, INTERVAL)


    def runPart(self):
        if self.__secondsSinceLastCheck < INTERVAL:
            time.sleep(1)
            self.__secondsSinceLastCheck += 1
            return
        logger.info("Check rss at " + self.__url)
        feed = feedparser.parse(self.__url)
        c = 0
        for item in feed["items"]:
            c += 1 if self.__rssAnalyzer.addToQueue(item["links"][0]["href"], self.__topic) else 0
        logger.info("Add " + str(c) + " urls to queue (" + self.__topic + ")")
        self.__secondsSinceLastCheck = 0


class UrlDownloaderController():

    def __init__(self, dir, workers=5):
        self.__dir = dir
        if not os.path.exists(dir):
            os.makedirs(dir)
        pageLogger = logging.getLogger('pageDownloader')
        pageLogger.setLevel(logging.ERROR)
        self.__pageDownloader = PageDownloader(logger=pageLogger, iframes=REMOVE, js=REMOVE)
        self.__logFile = open(os.path.join(dir, "urls.txt"), "a")
        self.__downloaders = []
        self.__pending = []
        self.__queue = Queue.Queue()
        for i in range(0, workers):
            d = UrlDownloader(i, self.__pageDownloader, self.__queue, self.__jobCallback)
            self.__downloaders.append(d)
            d.start()

    def addToQueue(self, url, topic):
        topicDir = os.path.join(self.__dir, topic)
        if not os.path.exists(topicDir):
            os.makedirs(topicDir)
        uuid = stringToDigest(url)
        idx = url.index("&url=") + 5
        url = url[idx:]
        if uuid in self.__pending:
            return False
        htmlFilename =  os.path.join(topicDir, uuid + ".html")
        txtFilename =  os.path.join(topicDir, uuid + ".txt")
        if not os.path.exists(htmlFilename) or not os.path.exists(txtFilename):
            self.__pending.append(uuid)
            self.__queue.put({"url": url, "html": htmlFilename, "txt": txtFilename, "uuid": uuid})
            self.__logFile.write(os.path.join(topic, uuid + ".html") + " : " + url + "\n")
            return True
        return False

    def __jobCallback(self, uuid):
        self.__pending.remove(uuid)

    def stop(self):
        for d in self.__downloaders:
            d.stop()

    def onWorkerStop(self, worker):
        notingToDo = True
        for w in self.__downloaders:
            if w.isAlive():
                notingToDo = False
        if notingToDo:
            self.__logFile.close()


class UrlDownloader(StoppableThread):

    def __init__(self, id, pageDownloader, queue, callback):
        StoppableThread.__init__(self, "URLDownloader" + str(id))
        self.__pageDownloader = pageDownloader
        self.__queue = queue
        self.__callback = callback

    def runPart(self):
        try:
            job = self.__queue.get()
            urlAddr = job["url"]
            outputHtmlFilename = job["html"]
            outputTxtFilename = job["txt"]
            logger.info("Download: " + urlAddr)
            self.__pageDownloader.download(urlAddr, outputHtmlFilename)
            extractor = Extractor(extractor='ArticleExtractor', url=urlAddr)
            f = codecs.open(outputTxtFilename, "w", "utf-8")
            f.write(extractor.getText())
            f.close()
        except Empty:
            return
        except BaseException as e:
            logger.exception("Cannot download " + urlAddr)
        self.__callback(job["uuid"])

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__ctrl.onWorkerStop(self)

if __name__ == "__main__":
    RssAnalyzer("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/googlenews/")