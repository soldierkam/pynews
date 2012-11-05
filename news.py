from nltk import PorterStemmer, FreqDist
from nltk.classify import apply_features
from nltk.corpus import stopwords, LazyCorpusLoader, CategorizedPlaintextCorpusReader
from logger import logger
from nltk import NaiveBayesClassifier, ConfusionMatrix
import random, nltk

__author__ = 'soldier'

import os

newsCorpus = LazyCorpusLoader('news_corpus', CategorizedPlaintextCorpusReader, '(data).*', cat_file='cats.txt')

class FeatureGenerator():

    def __init__(self, freqDists):
        self.__words = set()
        for label, fd in freqDists.items():
            for w in fd.keys()[:1500]:
                self.__words.add(w)

    def __call__(self, document):
        doc = [w.lower() for w in document.split()]
        return dict([("contains(%s)" % w, w in doc) for w in self.__words])


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
        documentsWithLabel = [(document, label) for label in self.klasses() for url, document in self.documents(klassId).items()]
        random.shuffle(documentsWithLabel)
        featuresGenerator = FeatureGenerator(freqDists)
        testset = apply_features(featuresGenerator, documentsWithLabel[:1000])
        trainset = apply_features(featuresGenerator, documentsWithLabel[1000:])
        self.__classifier = NaiveBayesClassifier.train(trainset)
        logger.info("Accuracy: " + str(nltk.classify.accuracy(self.__classifier, testset)))
        ref = [label for features, label in testset]
        test = [self.__classifier.classify(features) for features, cat in testset]
        logger.info("\n" + ConfusionMatrix(ref, test).pp())



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
