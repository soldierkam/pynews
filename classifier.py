
from clustering import DocumentSizeClustering
from news import NewsClassificator
import os

class TxtClassificatorWrapper():

    def __init__(self):
        if "fake_run" in os.environ:
            self.__i = 0
            return
        self.__documentSizeClassificator = DocumentSizeClustering("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/clusteringData.db")
        self.__newsClassificator = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews2/", doTest=False)

    def classify(self, txt):
        if "fake_run" in os.environ:
            self.__i += 1
            self.__i = self.__i % 2
            return ["long", "p" if self.__i == 1 else "b"]
        return self.__documentSizeClassificator.classify(txt), self.__newsClassificator.classify(txt)

    def probDist(self, txt):
        return self.__newsClassificator.prob_classify(txt)