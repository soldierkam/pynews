# -*- coding: utf-8 *-*
import threading
from nltk import cluster
from nltk.cluster import euclidean_distance
from numpy import array
import shelve
import re
from nltk.corpus import stopwords
from lang import LangDetect
from logger import logger

def getWords(data):
    t = [item["text"] for key, item in data.items()]
    words = set()
    ignored = stopwords.words('english')
    for text in t:
        for word in text.split():
            word = word.lower()
            if word.isalpha() and word not in ignored:
                words.add(word)
    return words


def features(text):
    words = text.split()
    wordsCount = len(words)
    sentencesCount = text.count(".") - 2 * text.count("...")
    #pCount = text.count("\n\n")
    quoteCount = 0
    for quote in re.compile("\"[^\"]+\"").findall(text):
        quoteCount += 1 if quote.count(" ") >= 2 else 0
        #colonCount = text.count(":")
    semicolonCount = text.count(",")
    return array([sentencesCount, wordsCount, quoteCount, semicolonCount])



class DocumentSizeClustering():

    def __init__(self, filename = "/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets/cache"):
        logger.info("Start building " + self.__class__.__name__)
        self.__mutex = threading.Semaphore()
        data = shelve.open(filename, flag="r")
        langDetect = LangDetect.instance()
        vectors = [features(item["text"]) for digest, item in data.items() if item["text"] and item["text"] != "ERROR" and langDetect.detect(item["text"]) is "en"]
        self.__clusterer = cluster.KMeansClusterer(3, euclidean_distance, initial_means=[[10,40,0,1],[30,500,0,30],[120,1500,15,50]])
        self.__clusterer.cluster(vectors)
        klassIdToSize = {"0": 0, "1": 0, "2": 0}
        klassIdToWordsCount = {"0": 0, "1": 0, "2": 0}
        for item in data.itervalues():
            text = item["text"]
            if text and text != "ERROR":
                feat = features(text)
                klass = str(self.__clusterer.classify(feat))
                klassIdToSize[klass] += 1
                klassIdToWordsCount[klass] += len(text.split())
        data.close()
        results = []
        for klassId in ["0", "1", "2"]:
            meanWordsInKlass = klassIdToWordsCount[klassId] / klassIdToSize[klassId] if klassIdToSize[klassId] != 0 else 0
            results.append({"klass": klassId, "mean" : meanWordsInKlass})
        logger.info("Clustering results: " + str(results))
        sortedKlass = sorted(results, lambda x,y: x["mean"] < y["mean"])
        self.__klassIdToLabel = {klassIdWithLabel[0]: klassIdWithLabel[1] for klassIdWithLabel in zip([item["klass"] for item in sortedKlass], ["short", "medium", "long"])}

    def classify(self, document):
        try:
            self.__mutex.acquire()
            docClass = self.__clusterer.classify(features(document))
            return self.__klassIdToLabel[str(docClass)]
        finally:
            self.__mutex.release()

if __name__ == "__main__":
    DocumentSizeClustering()
