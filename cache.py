from datetime import datetime, timedelta
import threading
from tools import stringToDigest

class CacheOp():

    def __init__(self, cache, key, fun, timedelta=timedelta(hours=24), **args):
        self.__cache = cache
        self.__key = stringToDigest(unicode(key))
        self.__fun = fun
        self.__args = args
        self.__mutext = threading.Lock()
        self.__maxTimeDelta = timedelta

    def get(self):
        try:
            self.__mutext.acquire()
            if not self.__cache.has_key(self.__key):
                self.__callAndPut()
            cached = self.__cache[self.__key]
            putDate = cached["time"]
            delta = datetime.now() - putDate
            if delta > self.__maxTimeDelta:
                self.__callAndPut()
            return self.__cache[self.__key]["entry"]
        finally:
            self.__mutext.release()

    def __callAndPut(self):
        self.__cache[self.__key] = {"entry": self.__fun(**self.__args), "time": datetime.now()}