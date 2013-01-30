# -*- coding: utf-8 *-*
import codecs
import cookielib

from datetime import datetime
from threading import Thread, Event
import threading
import urllib2
from urllib2 import HTTPCookieProcessor, HTTPRedirectHandler
from inliner import Content
from logger import logger
import time, os, random
from hashlib import sha1

class RateMonitor():

    def __init__(self):
        self.__bucket = {}
        self.__sec = -1
        for i in range(0, 60):
            self.__bucket[i] = 0
        self.__rounds = 0
        self.__i = 0
        self.__start = datetime.now()

    def hit(self, c=1):
        sec = datetime.now().second
        if sec != self.__sec:
            self.__bucket[sec] = 0
            self.__sec = sec
            self.__rounds += 1
        self.__bucket[sec] = self.__bucket[sec] + c
        self.__i += 1

    def value(self):
        sec = datetime.now().second
        assert sec >=0 and sec <=59
        return self.__bucket[sec]

    def lastAvg(self):
        if self.__rounds < len(self.__bucket):
            return None
        s = 0.0
        for v in self.__bucket.values():
            s += v
        return round(s / 60, 2)

    def avg(self):
        diff = datetime.now() - self.__start
        return self.__i / diff.seconds

class NothingToDo(Exception):

    def __init(self):
        pass


class RssDataReader():

    def __init__(self, dir, testDir=None):
        self.__dir = dir
        self.__testDir = testDir or dir
        self.__filenameToUrl = self._readLogFile()
        #self.__fixRepo()

    def __fixRepo(self):
        for filename, url in self.__filenameToUrl.iteritems():
            try:
                txtfullpath = os.path.join(self.__dir, filename)
                htmlfullpath = txtfullpath[:-4] + ".html"
                if not os.path.exists(htmlfullpath):
                    logger.info("Download: " + url + " to " + htmlfullpath)
                    content, url = self.__download(url)
                    f = open(htmlfullpath, "w")
                    f.write(content.data())
                    f.close()
            except:
                logger.exception("Error")

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


    def _readLogFile(self):
        results = {}
        self._parseLog(os.path.join(self.__dir, "urls.txt"), results)
        self._parseLog(os.path.join(self.__testDir, "urls.txt"), results)
        return results

    def _parseLog(self, file, results):
        f = open(file)
        for line in f.readlines():
            filename, url = self._parseLogLine(line)
            if filename is not None and url is not None:
                results[filename] = url
        f.close()

    def _parseLogLine(self, line):
        if " : " in line:
            parts = line.split(" : ")
            filename = parts[0].strip().replace(".html", ".txt")
            url = parts[1].strip()
            return filename, url
        else:
            logger.error(u"Wrong line: " + unicode(line))
            return None, None

    def documents(self, klassId, html=False):
        l = self._getDocuments(klassId, self.__dir, html)
        #logger.info(u"Read %d documents from %s (%s)" % (len(l), self.__dir, klassId))
        return l

    def _testDocuments(self, klassId, html=False):
        l = self._getDocuments(klassId, self.__testDir, html)
        #keys = [url for url, doc in l]
        #random.shuffle(l)
        #keys = keys[:n]
        #l = {key: l[key] for key in keys}
        #logger.info("Read %d test documents from %s (%s)" % (len(l), self.__testDir, klassId))
        return l


    def _getDocuments(self, klassId, dir, html=False, limit=None):
        klassDir = os.path.join(dir, klassId)
        counter = 0
        for file in os.listdir(klassDir):
            if file.endswith(".txt"):
                #fd = open(os.path.join(klassDir, file))
                relative = os.path.join(klassId, file)
                filename = os.path.join(klassDir, file)
                if html:
                    filename = filename[:-4] + ".html"
                    if not os.path.exists(filename):
                        continue
                    fd = open(filename, "r")
                else:
                    fd = codecs.open(filename, "r", encoding="UTF-8")
                if not self.__filenameToUrl.has_key(relative):
                    continue
                url = self.__filenameToUrl[relative]
                result = fd.read()
                if not result:
                    continue
                yield url, result
                fd.close()
                counter += 1
                if limit is not None and counter >= limit:
                    break


    def klasses(self, ignoreKlass = [], includeKlass = None):
        results = []
        for dirEntry in os.listdir(self.__dir):
            if os.path.isdir(os.path.join(self.__dir, dirEntry)) and dirEntry not in ignoreKlass and (includeKlass is None or dirEntry in includeKlass):
                results.append(dirEntry)
        return results

class StoppableThread(Thread):

    def __init__(self, name = None):
        name = name or self.__class__.__name__
        Thread.__init__(self, name=name)
        self.__stop = Event()
        self.error = Event()
        self.__pauseEvent = Event()
        self.__msgCount = 0

    def stop(self):
        self.__stop.set()

    def isStopping(self):
        return self.__stop.isSet()

    def pauseJob(self):
        self.__msgCount = 0
        self.__pauseEvent.set()

    def continueJob(self):
        self.__pauseEvent.clear()

    def isPaused(self):
        return self.__pauseEvent.isSet()

    def run(self):
        try:
            self.atBegin()
            while not self.__stop.isSet():
                try:
                    #logger.debug("Check is paused?")
                    if self.__pauseEvent.isSet():
                        self.onPause()
                        time.sleep(1)
                        continue
                    #logger.debug("Run part...")
                    self.runPart()
                except NothingToDo:
                    logger.info("nothing to do")
                    break
                except BaseException as exc:
                    self.error.set()
                    logger.exception("error")
                    break
            self.atEnd()
        except:
            logger.exception("Fatal error")

    def onPause(self):
        if self.__msgCount % 60 == 0:
            logger.debug("Paused")
        self.__msgCount += 1
        self.__msgCount %= 60

    def runPart(self):
        pass

    def atBegin(self):
        pass

    def atEnd(self):
        logger.info("exiting... (%d)" % threading.activeCount())
        pass

def stringToDigest(string):
    return sha1(string).hexdigest()

