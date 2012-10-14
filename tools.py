__author__ = 'soldier'

from datetime import datetime

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
