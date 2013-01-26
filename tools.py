# -*- coding: utf-8 *-*
import codecs

from datetime import datetime
from threading import Thread, Event
import threading
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
            filename = parts[0].strip().split("/")[1].replace(".html", ".txt")
            url = parts[1].strip()
            return filename, url
        else:
            logger.error(u"Wrong line: " + unicode(line))
            return None, None

    def documents(self, klassId):
        l = self._getDocuments(klassId, self.__dir)
        logger.info(u"Read %d documents from %s (%s)" % (len(l), self.__dir, klassId))
        return l

    def _testDocuments(self, klassId, n=1000):
        l = self._getDocuments(klassId, self.__testDir, n * 3)
        keys = l.keys()
        random.shuffle(keys)
        keys = keys[:n]
        l = {key: l[key] for key in keys}
        logger.info("Read %d test documents from %s (%s)" % (len(l), self.__testDir, klassId))
        return l


    def _getDocuments(self, klassId, dir, limit=None):
        klassDir = os.path.join(dir, klassId)
        results = {}
        counter = 0
        for file in os.listdir(klassDir):
            if file.endswith(".txt"):
                #fd = open(os.path.join(klassDir, file))
                fd = codecs.open(os.path.join(klassDir, file), "r", encoding="UTF-8")
                if not self.__filenameToUrl.has_key(file):
                    continue
                url = self.__filenameToUrl[file]
                results[url] = fd.read()
                fd.close()
                counter += 1
                if limit is not None and counter >= limit:
                    break
        return results

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

