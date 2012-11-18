# -*- coding: utf-8 *-*
import shelve

__author__ = 'soldier'

import sys
import tweetstream
import struct
import gzip
import pickle
import os
import re
from os import path
from datetime import datetime
from datetime import timedelta
from logger import logger
from Queue import Queue, Full, Empty
from threading import Thread, Event
from tools import RateMonitor
from dateutil import parser


filenameSCPattern = re.compile(r"s=([^,]+),c=([^,]+).tweets")
filenameSCEPattern = re.compile(r"s=([^,]+),c=([^,]+),e=([^,]+).tweets")

def filenameToKey(filename):
    return os.path.basename(filename)

def storeFileInfo(infoLog, filename, startTime, count, endTime):
    key = filenameToKey(filename)
    value = {"start" : startTime, "tweets_count": count, "end" : endTime}
    logger.info("Save " + str(value) + " to " + str(key))
    infoLog[key] = value

class TweetIterator(object):

    def __init__(self, filesNames, infoLog):
        self.__infoLog = infoLog
        self.__fileIdx = -1
        self.__tweetIdx = 0
        self.__tweetInFileCounter = 0
        self.__file = None
        self.__count = 0
        fixedFilenames = []
        for filename in filesNames:
            val = self.__tweetsInFile(filename)
            startTime = self.__startTime(filename)
            endTime = self.__endTime(filename)
            self.__count += val
            filename = self.__fixFilename(filename)
            fixedFilenames.append(filename)
            key = filenameToKey(filename)
            if not self.__infoLog.has_key(key):
                storeFileInfo(self.__infoLog, filename, startTime, val, endTime)
            #else:
            #    storeFileInfo(self.__infoLog, filename, startTime, val, endTime)
        self.__all = fixedFilenames

    def __isLastFile(self):
        return self.__fileIdx == len(self.__all) - 1

    def __getNextFile(self):
        if self.__file:
            self.__file.close()
        self.__file = None
        self.__fileIdx += 1
        self.__tweetInFileCounter = 0
        filename = self.__all[self.__fileIdx] if self.__fileIdx < len(self.__all) else None
        if filename:
            logger.info("Open next " + filename)
            self.__file = gzip.open(filename)
        else:
            logger.info("Last file reached")
            self.__file = None
            raise ValueError()

    def __fixFilename(self, filename):
        newFilename = None
        key = os.path.basename(filename)
        dir = os.path.dirname(filename)
        matcher = filenameSCEPattern.search(key)
        if matcher:
            newFilename = matcher.group(1) + ".tweets"
        if not newFilename:
            matcher = filenameSCPattern.search(key)
            if matcher:
                newFilename = matcher.group(1) + ".tweets"
        if not newFilename:
            newFilename = key
        if newFilename is None and "=" in filename:
            raise ValueError(filename)
        newFilename = os.path.join(dir, newFilename)
        if filename != newFilename:
            logger.info("Rename " + filename + " to " + newFilename)
            os.rename(filename, newFilename)
        return newFilename

    def __iter__(self):
        return self

    def position(self):
        return self.__tweetIdx

    def count(self):
        return self.__count

    def countInCurrentFile(self):
        return self.__tweetsInFile(self.__file)

    def currentFile(self):
        return self.__fileIdx + 1

    def filesCount(self):
        return len(self.__all)

    def __tweetsInFile(self, filename):
        key = filenameToKey(filename)
        if self.__infoLog.has_key(key):
            return self.__infoLog[key]["tweets_count"]
        return self.__countTweetsInFile(filename)

    def __startTime(self, filename):
        key = filenameToKey(filename)
        if self.__infoLog.has_key(key):
            return self.__infoLog[key]["start"]
        matcher = filenameSCEPattern.search(key)
        if matcher:
            return parser.parse(matcher.group(1))
        matcher = filenameSCPattern.search(key)
        if matcher:
            return parser.parse(matcher.group(1))
        return parser.parse(key.replace(".tweets", ""))

    def __endTime(self, filename):
        key = filenameToKey(filename)
        if self.__infoLog.has_key(key):
            return self.__infoLog[key]["end"]
        matcher = filenameSCEPattern.search(key)
        if matcher:
            return parser.parse(matcher.group(3))
        logger.warning(u"Cannot fine end time for " + unicode(filename))
        return None

    def __countTweetsInFile(self, filename):
        tweetCount = 0
        logger.info("Start counting " + str(filename))
        file =  gzip.open(filename, "r")
        file.seek(0)
        while True:
            buf = file.read(4)
            if not buf:
                break
            size = struct.unpack('i', buf)[0]
            data = file.read(size)
            if len(data) != size:
                raise ValueError(str(len(data)) + " != " + str(size))
            tweetCount += 1
        logger.info("End counting " + str(filename) + ": " + str(tweetCount))
        return tweetCount

    def next(self):
        if not self.__file:
            self.__getNextFile()
        buf = self.__file.read(4)
        if not buf :
            if self.__isLastFile():
                raise StopIteration()
            else:
                #raise StopIteration()
                self.__getNextFile()
                buf = self.__file.read(4)
        size = struct.unpack('i', buf)[0]
        data = self.__file.read(size)
        if len(data) != size:
            raise ValueError(str(len(data)) + " != " + str(size))
        tweet = pickle.loads(data)
        self.__tweetIdx += 1
        self.__tweetInFileCounter += 1
        return tweet

class QueueIterator():

    def __init__(self, q, wrapper, rateMon):
        self.__q = q
        self.__wrapper = wrapper
        self.__monitor = rateMon

    def __iter__(self):
        return self

    def next(self):
        while True:
            try:
                o = self.__q.get(timeout=5)
                self.__monitor.hit()
                return o
            except Empty:
                self.__wrapper.reset()

class StreamWrapperHelper(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.__stop = Event()
        self.__queue = queue
        self.__full = Event()
        self.start()

    def run(self):
        while True:
            if self.__stop.isSet():
                return
            stream = tweetstream.SampleStream(u'soldierkam', os.environ["PASSWORD"])
            for s in stream:
                try:
                    if self.__stop.isSet():
                        stream.close()
                        return
                    self.__queue.put(s, block=False)
                    self.__full.clear()
                except Full:
                    if not self.__full.isSet():
                        logger.warn("Queue is full!!")
                        self.__full.set()

    def close(self):
        self.__stop.set()

class StreamWrapper:

    def __init__(self):
        self.__rateMon = RateMonitor()
        self.__queue = Queue(maxsize=300)
        self.__helper = StreamWrapperHelper(self.__queue)

    def __iter__(self):
        return QueueIterator(self.__queue, self, self.__rateMon)

    def reset(self):
        logger.info("Reset stream wrapper")
        self.__helper.close()
        self.__helper = StreamWrapperHelper(self.__queue)

    def close(self):
        self.__helper.close()

    def getCurrentRate(self):
        return self.__rateMon.value()

    def getAvgRate(self):
        return self.__rateMon.lastAvg()

class Manager:

    def __init__(self, dir):
        self.__dir = dir
        self.__infoLog = shelve.open(os.path.join(dir, "infoLog.db"))

    def store(self):
        filename = self.filenameWrite(dir)
        f_out = gzip.open(filename, 'wb')
        start = datetime.now()
        dayDelta = timedelta(hours=5)
        i = 0
        tweetInStreamCounter = 0
        tweetInFileCounter = 0
        stream = None
        try:
            stream = StreamWrapper()
            for s in stream:
                data = pickle.dumps(s)
                f_out.write(struct.pack('i', len(data)))
                f_out.write(data)
                if i > 100:
                    i = 0
                    print "Rate: " + str(stream.getCurrentRate()) + "/sec Avg: " + str(stream.getAvgRate()) + "/sec"
                    delta = datetime.now() - start
                    if delta > dayDelta:
                        print u"Close file " + filename + u" (" + unicode(tweetInStreamCounter) + u")"
                        f_out.close()
                        self.__saveInfoAboutFile(filename, start, tweetInFileCounter)
                        filename = self.filenameWrite(dir)
                        f_out = gzip.open(filename, 'wb')
                        start = datetime.now()
                        tweetInFileCounter = 0
                else:
                    i += 1
                tweetInStreamCounter += 1
                tweetInFileCounter += 1
        except KeyboardInterrupt:
            print u"Close file " + filename + u" (" + unicode(tweetInStreamCounter) + u")"
            self.__callQuiet(f_out.close)
            if stream:
                self.__callQuiet(stream.close)

    def __callQuiet(self, fun):
        try:
            fun()
        except BaseException:
            pass

    def __saveInfoAboutFile(self, filename, start, tweetsCount):
        storeFileInfo(self.__infoLog, filename, start, tweetsCount, datetime.now())

    def restore(self, lastOnly=False):
        filesNames = self.filenameRead(self.__dir)
        logger.info(u"Open files: " + unicode('\n'.join(map(str, filesNames))))
        filesNames = [filesNames[-1]] if lastOnly else filesNames
        return TweetIterator(filesNames, self.__infoLog)

    def filenameWrite(self, dir):
        if not path.exists(dir):
            os.makedirs(dir)
        now = datetime.now()
        return path.join(dir, unicode(now) + u".tweets")

    def filenameRead(self, filename):
        if path.isfile(filename):
            return [filename]

        result = []
        for dirname, dirnames, filenames in os.walk(filename):
            for filename in filenames:
                if filename.endswith(u".tweets"):
                    file = path.join(dirname, filename)
                    result.append(file)
        if not result:
            raise Exception("No file in " + filename)
        return sorted(result, key=os.path.getmtime)

if __name__ == "__main__":
    dir = sys.argv[2]
    method = sys.argv[1]
    m = Manager(dir)
    if method == u"s":
        m.store(dir)
    elif method == u"r":
        for t in m.restore():
            print t
    else:
        raise ValueError("Method")
