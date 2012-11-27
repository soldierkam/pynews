import threading

class CacheOp():

    def __init__(self, cache, key, fun, **args):
        self.__cache = cache
        self.__key = key
        self.__fun = fun
        self.__args = args
        self.__mutext = threading.Lock()

    def get(self):
        try:
            self.__mutext.acquire()
            if not self.__cache.has_key(self.__key):
                self.__cache[self.__key] = self.__fun(self.__args)
            return self.__cache[self.__key]
        finally:
            self.__mutext.release()