import threading
from clustering import DocumentSizeClustering
from logger import logger
from news import NewsClassificator
import os
from tools import StoppableThread, NothingToDo

class TxtClassificatorWrapper():

    _instance = None
    _mutex = threading.Semaphore()

    def __init__(self):
        if "fake_run" in os.environ:
            self.__i = 0
            return
        self.__documentSizeClassificator = DocumentSizeClustering("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/clusteringData.db")
        self.__newsClassificator = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews-all/", doTest=False)


    @staticmethod
    def instance():
        if TxtClassificatorWrapper._instance is not None:
            return TxtClassificatorWrapper._instance
        try:
            TxtClassificatorWrapper._mutex.acquire()
            if TxtClassificatorWrapper._instance is None:
                TxtClassificatorWrapper._instance = TxtClassificatorWrapper()
            return TxtClassificatorWrapper._instance
        finally:
            TxtClassificatorWrapper._mutex.release()

    def classify(self, txt):
        if "fake_run" in os.environ:
            self.__i += 1
            self.__i = self.__i % 3
            if self.__i == 1:
                return ["long", "p"]
            elif self.__i == 2:
                return ["long", "b"]
            else:
                return ["short", "p"]
        try:
            return self.__documentSizeClassificator.classify(txt), self.__newsClassificator.classify(txt)
        except BaseException as e:
            logger.exception(u"Error in \"" + unicode(txt) + u"\"")
            raise e

    def probDist(self, txt):
        if "fake_run" in os.environ:
            return None
        return self.__newsClassificator.prob_classify(txt)

class Init(StoppableThread):

    def runPart(self):
        i = TxtClassificatorWrapper.instance()
        raise NothingToDo()

    def atEnd(self):
        StoppableThread.atEnd(self)
        init = None

init = Init()
init.start()