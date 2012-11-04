from nltk import PorterStemmer, FreqDist
from nltk.corpus import stopwords
from logger import logger

__author__ = 'soldier'

import os

class NewsClassificator():

    def __init__(self, dir):
        self.__dir = dir
        self.__filenameToUrl = self.__readLogFile()
        self.__stemmer = PorterStemmer()
        freqDists = {}
        ignore = stopwords.words('english')
        for klassId in self.klasses():
            freqDist = FreqDist()
            for url, txt in self.documents(klassId).items():
                for part in txt.split():
                    part = part.lower()
                    if part.isalnum() and part not in ignore:
                        freqDist.inc(self.__stemmer.stem(part))
            freqDists[klassId] = freqDist
        for klassId, freqDist in freqDists.items():
            logger.info("Klass id " + klassId + ": " + str(freqDist.keys()[:1000]))


    def __readLogFile(self):
        results = {}
        f = open(os.path.join(self.__dir, "urls.txt"))
        for line in f.readlines():
            if line.index(" : ") != 1:
                parts = line.split(" : ")
                filename = parts[0].strip().split("/")[1].replace(".html", ".txt")
                url = parts[1].strip()
                results[filename] = url
        f.close()
        return results

    def documents(self, klassId):
        klassDir = os.path.join(self.__dir, klassId)
        results = {}
        for file in os.listdir(klassDir):
            if file.endswith(".txt"):
                fd = open(os.path.join(klassDir, file))
                url = self.__filenameToUrl[file]
                results[url] = fd.read()
                fd.close()
        return results

    def klasses(self):
        results = []
        for dirEntry in os.listdir(self.__dir):
            if os.path.isdir(os.path.join(self.__dir, dirEntry)):
                results.append(dirEntry)
        return results

if __name__ == "__main__":
    nc = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/googlenews/")
