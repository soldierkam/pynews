import Queue
from Queue import Empty
import codecs
import cookielib
import time
import logging
import os
import urllib
from urllib2 import HTTPRedirectHandler, HTTPCookieProcessor
import chardet
from nltk.downloader import urllib2
from inliner import REMOVE, DO_NTH, Content
from tools import stringToDigest
import random
import feedparser
from boilerpipe.extract import Extractor
from inliner import Downloader as PageDownloader
from tools import StoppableThread
from logger import logger


#w - specifies the world topic
WORLD="w"
#b - specifies the business topic
BUSINESS="b"
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

    def __init__(self, dir, langs = ["us", "uk", "au", "en_ca"], langAsKlass=False):
        self.__downloader = UrlDownloaderController(dir, langAsKlass=langAsKlass)
        self.__pollers = []
        for lang in langs:
            for topic in [HEADLINES, WORLD, NATION, SCIENCE, TECHNOLOGY, ELECTIONS, POLITICS, ENTERTAINMENT, SPORT, HEALTH, BUSINESS]:
                url = GoogleNewsUrl().setEdition(lang).setTopic(topic).build()
                p = RssPoller(url, topic, lang, self)
                self.__pollers.append(p)
                p.start()

    def addToQueue(self, url, edition, topic):
        return self.__downloader.addToQueue(url, edition, topic)

INTERVAL = 300

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
            c += 1 if self.__rssAnalyzer.addToQueue(item["links"][0]["href"], self.__lang, self.__topic) else 0
        logger.info("Add " + str(c) + " urls to queue (" + self.__topic + ")")
        self.__secondsSinceLastCheck = 0


class UrlDownloaderController():

    def __init__(self, dir, workers=5, langAsKlass= False):
        self.__langAsKlass = langAsKlass
        self.__dir = dir
        if not os.path.exists(dir):
            os.makedirs(dir)
        pageLogger = logging.getLogger('pageDownloader')
        pageLogger.setLevel(logging.ERROR)
        self.__pageDownloader = PageDownloader(logger=pageLogger, iframes=REMOVE, js=REMOVE, img=DO_NTH)
        self.__logFile = open(os.path.join(dir, "urls.txt"), "a")
        self.__downloaders = []
        self.__pending = []
        self.__queue = Queue.Queue()
        for i in range(0, workers):
            d = UrlDownloader(i, self.__pageDownloader, self.__queue, self.__jobCallback)
            self.__downloaders.append(d)
            d.start()

    def addToQueue(self, url, edition, topic):
        klassDir = os.path.join(self.__dir, edition if self.__langAsKlass else topic)
        if not os.path.exists(klassDir):
            os.makedirs(klassDir)
        uuid = stringToDigest(url)
        idx = url.index("&url=") + 5
        url = url[idx:]
        url = urllib.unquote(url)
        htmlFilename =  os.path.join(klassDir, uuid + ".html")
        txtFilename =  os.path.join(klassDir, uuid + ".txt")
        if uuid in self.__pending or os.path.exists(txtFilename):
            return False
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


    def __download(self, url):
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(HTTPRedirectHandler(), HTTPCookieProcessor(cj))
        opener.addheaders = [('User-agent', 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)')]
        ct = opener.open(url)
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

    def runPart(self):
        job = None
        urlAddr = None
        try:
            job = self.__queue.get()
            urlAddr = job["url"]
            outputHtmlFilename = job["html"]
            outputTxtFilename = job["txt"]
            logger.info("Download: " + urlAddr)
            #self.__pageDownloader.download(urlAddr, outputHtmlFilename)
            content, url = self.__download(urlAddr)
            encoding = content.encoding() or chardet.detect(content.data())['encoding'] or "ISO-8859-1"
            data = unicode(content.data(), encoding)
            extractor = Extractor(extractor='ArticleExtractor', html=data)
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
    RssAnalyzer("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/googlenews-27.01/")