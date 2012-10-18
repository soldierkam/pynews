__author__ = 'soldier'

from datetime import datetime
from threading import Thread, Event
from logger import logger
import time
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

def stringToDigest(string):
    return sha1(string).hexdigest()