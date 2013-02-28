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
from random import choice

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

def avgFun(a):
    return a[0] / a[1]


def calcDiv(vectors):
    sumV = [0,0,0,0]
    maxV = [0,0,0,0]
    minV = [10000, 10000, 10000, 10000]
    for v in vectors:
        sumV = map(sum, zip(sumV, v))
        minV = map(min, zip(minV, v))
        maxV = map(max, zip(maxV, v))
    avgV = map(avgFun, zip(sumV, [len(vectors)] * 4))
    logger.info(u"Avg: " + unicode(avgV) + u" Min: " + unicode(minV) + u" Max: " + unicode(maxV))
    return array(maxV)

#def normalize(toNormalize, maxV):
#    if not isinstance(toNormalize, list):
#        return toNormalize / maxV
#    newVectors = []
#    for v in toNormalize:
#        newVectors.append( v / maxV)
#    return newVectors


class DocumentSizeClustering():

    def __init__(self, filename = "/run/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/clusteringData.db"):
        logger.info("Start building " + self.__class__.__name__)
        self.__mutex = threading.Semaphore()
        data = shelve.open(filename, protocol=-1, flag="r")
        langDetect = LangDetect.instance()
        vectors = [features(item["text"]) for digest, item in data.items() if item["text"] and item["text"] != "ERROR" and langDetect.detect(item["text"]) is "en"]
        self.__maxV = calcDiv(vectors)
        #vectors = normalize(vectors, self.__maxV)
        means = [array([10, 40, 0, 1]), array([30, 340, 2, 30]), array([120, 1500, 15, 50])]
        self.__clusterer = cluster.KMeansClusterer(3, euclidean_distance, initial_means=means, avoid_empty_clusters=True)
        self.__clusterer.cluster(vectors)
        klassIdToSize = {"0": 0, "1": 0, "2": 0}
        klassIdToWordsCount = {"0": 0, "1": 0, "2": 0}
        for item in data.itervalues():
            text = item["text"]
            if text and text != "ERROR":
                feat = features(text)
                #feat = normalize(feat, self.__maxV)
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
            feat = features(document)
            #feat = normalize(feat, self.__maxV)
            docClass = self.__clusterer.classify(feat)
            return self.__klassIdToLabel[str(docClass)]
        finally:
            self.__mutex.release()

if __name__ == "__main__":
    c = DocumentSizeClustering()
    langDetect = LangDetect.instance()
    data = shelve.open("/run/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/clusteringData.db", protocol=-1, flag="r")
    print "Documents: " + str(len(data))
    position = 0
    labels = {"short": 0, "medium": 0, "long": 0}
    input = []
    for digest, item in data.items():
        if item["text"] and item["text"] != "ERROR" and langDetect.detect(item["text"]) is "en":
            input.append(item)
    testItems = input
    #testItems = []
    #for i in range(0, 150):
    #    e = choice(input)
    #    input.remove(e)
    #    testItems.append(e)

    stats = {"short": {"min":10000, "max":0}, "medium":{"min":10000, "max":0}, "long": {"min":10000, "max":0}}
    for item in testItems:
        position += 1
        ignore = False
        for i in ["Load new Tweets", "Comment is free", "Embed this Photo", "Suggested Language"]:
            ignore = ignore or item["text"].startswith(i)
        label = c.classify(item["text"])
        labels[label] += 1
        #if ignore and label is "short":
        #    print str(position) + " Skip one"
        #    continue
        feat = features(item["text"])
        stats[label]["min"] = min(stats[label]["min"], feat[1])
        stats[label]["max"] = max(stats[label]["max"], feat[1])
        print str(position) + "\t" + item["url"]  + "\t"+ label +"\t" + item["text"][:200].replace("\n", " ").replace("\t", " ") + "... " + str(feat)
    print labels
    print stats
    data.close()