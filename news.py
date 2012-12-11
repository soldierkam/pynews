import threading
from nltk import PorterStemmer, FreqDist
from nltk.classify import apply_features
from nltk.corpus import stopwords, LazyCorpusLoader, CategorizedPlaintextCorpusReader
import operator
from google_rss import HEADLINES, SPOTLIGHT, NATION, SPORT, POLITICS
from logger import logger
from nltk import NaiveBayesClassifier, ConfusionMatrix
import random, nltk
import numpy as np

__author__ = 'soldier'

import os

newsCorpus = LazyCorpusLoader('news_corpus', CategorizedPlaintextCorpusReader, '(data).*', cat_file='cats.txt')
_stemmer = PorterStemmer()

def tokenize(txt):
    return [ _stemmer.stem(token.lower()) for token in nltk.wordpunct_tokenize(txt)]

class FeatureGenerator():

    def __init__(self, freqDists, featureFd, klassSize):
        featureToMaxRatio = {feature: 0 for feature in featureFd.samples()}
        for feature in featureFd.samples():
            featureRatio = []
            for klass, fd1 in freqDists.items():
                frequency = fd1.freq(feature) * klassSize[klass]
                for fd2 in freqDists.values():
                    if fd2 is not fd1:
                        frequency2 = fd2.freq(feature) * klassSize[klass]
                        featureRatio.append(frequency / frequency2 if frequency2 > 0 else 0)
            featureToMaxRatio[feature] = max(featureRatio)
        bestFeatures = sorted(featureToMaxRatio.iteritems(), key=operator.itemgetter(1), reverse=True)
        logger.info(u"Best features: " + unicode(bestFeatures[:100]))
        self.__features = [item[0] for item in bestFeatures[:3000]]

    def __getFeatures(self, document):
        tokens = tokenize(document)
        doc = tokens#[tokens + nltk.bigrams(tokens)]
        return dict([("contains(%s)" % str(f), f in doc) for f in self.__features])

    def __call__(self, document):
        return self.__getFeatures(document)


class NewsClassificator():

    def __init__(self, dir, testDir=None, doTest = True, ignoreKlass = [], includeKlass = None):
        logger.info("Start building " + self.__class__.__name__)
        self.__mutex = threading.Semaphore()
        self.__dir = dir
        self.__testDir = testDir or dir
        self.__filenameToUrl = self.__readLogFile()
        freqDists = {}
        ignore = stopwords.words('english')
        featureFd = FreqDist()
        klassSize = {}
        for klassId in self.__klasses(ignoreKlass, includeKlass):
            freqDist = FreqDist()
            size = 0
            for url, txt in self.__documents(klassId).items():
                txt = tokenize(txt)
                size += 1
                for part in txt:
                    if part.isalnum() and part not in ignore:
                        freqDist.inc(part)
                        featureFd.inc(part)
                #for bigram in nltk.bigrams(txt):
                #    freqDist.inc(bigram)
                #    featureFd.inc(bigram)
            freqDists[klassId] = freqDist
            klassSize[klassId] = size

        documentsWithLabel = [(document, correctKlass) for correctKlass in self.__klasses(ignoreKlass, includeKlass) for url, document in self.__documents(correctKlass).items()]
        testDocumentsWithLabel = [(document, correctKlass) for correctKlass in self.__klasses(ignoreKlass, includeKlass) for url, document in self.__testDocuments(correctKlass).items()]
        random.shuffle(documentsWithLabel)

        self.__featuresGenerator = FeatureGenerator(freqDists, featureFd, klassSize)
        testset = apply_features(self.__featuresGenerator, testDocumentsWithLabel)
        trainset = apply_features(self.__featuresGenerator, documentsWithLabel)
        self.__classifier = NaiveBayesClassifier.train(trainset)
        logger.info(u"Classifier learned (set size=" + unicode(len(trainset)) + u")")
        if doTest:
            ref = [correctKlass for features, correctKlass in testset]
            test = [self.__classifier.classify(features) for features, cat in testset]
            #for correctKlass, klass, featuresWithLabel in zip(ref, test, testset):
            #    if correctKlass != klass:
            #        pd = self.__classifier.prob_classify(dict(featuresWithLabel[0]))
            #        labelProbList = sorted( [(sample, pd.logprob(sample)) for sample in pd.samples()], key=lambda x: x[1], reverse=True)
            #        logger.info( correctKlass + " as " + klass + ": " + str([(correctKlass, "%.2f" % prob) for correctKlass, prob in labelProbList]))
            #        logger.info([(key, value)for key, value in featuresWithLabel[0].items() if value > 0])
            #        logger.info(self.__findDocumentByKlassAndFeatures(correctKlass, featuresWithLabel[0]))
            logger.info("\n" + ConfusionMatrix(ref, test).pp())
            logger.info("Accuracy: " + str(nltk.classify.accuracy(self.__classifier, testset)))
            self.__classifier.show_most_informative_features(n=300)


    def classify(self, document):
        try:
            self.__mutex.acquire()
            return self.__classifier.classify(self.__featuresGenerator(document))
        finally:
            self.__mutex.release()

    def prob_classify(self, document):
        try:
            self.__mutex.acquire()
            return self.__classifier.prob_classify(self.__featuresGenerator(document))
        finally:
            self.__mutex.release()

    def __readLogFile(self):
        results = {}
        self.__parseLog(os.path.join(self.__dir, "urls.txt"), results)
        self.__parseLog(os.path.join(self.__testDir, "urls.txt"), results)
        return results

    def __parseLog(self, file, results):
        f = open(file)
        for line in f.readlines():
            filename, url = self.__parseLogLine(line)
            if filename is not None and url is not None:
                results[filename] = url
        f.close()

    def __parseLogLine(self, line):
        if " : " in line:
            parts = line.split(" : ")
            filename = parts[0].strip().split("/")[1].replace(".html", ".txt")
            url = parts[1].strip()
            return filename, url
        else:
            logger.error(u"Wrong line: " + unicode(line))
            return None, None

    def __documents(self, klassId):
        l = self.__getDocuments(klassId, self.__dir)
        logger.info(u"Read %d documents from %s (%s)" % (len(l), self.__dir, klassId))
        return l

    def __getDocuments(self, klassId, dir, limit=None):
        klassDir = os.path.join(dir, klassId)
        results = {}
        counter = 0
        for file in os.listdir(klassDir):
            if file.endswith(".txt"):
                fd = open(os.path.join(klassDir, file))
                url = self.__filenameToUrl[file]
                results[url] = fd.read()
                fd.close()
                counter += 1
                if limit is not None and counter >= limit:
                    break
        return results

    def __testDocuments(self, klassId, n=1000):
        l = self.__getDocuments(klassId, self.__testDir, n * 3)
        keys = l.keys()
        random.shuffle(keys)
        keys = keys[:n]
        l = {key: l[key] for key in keys}
        logger.info("Read %d test documents from %s (%s)" % (len(l), self.__testDir, klassId))
        return l

    def __findDocumentByKlassAndFeatures(self, klassId, features):
        docs = self.__documents(klassId)
        for url, doc in docs.items():
            docFeatures = self.__featuresGenerator(doc)
            if docFeatures == features:
                return doc
        return None

    def __klasses(self, ignoreKlass = [], includeKlass = None):
        results = []
        for dirEntry in os.listdir(self.__dir):
            if os.path.isdir(os.path.join(self.__dir, dirEntry)) and dirEntry not in ignoreKlass and (includeKlass is None or dirEntry in includeKlass):
                results.append(dirEntry)
        return results

if __name__ == "__main__":
    nc = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews/",
        testDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews/",
        doTest=True,
        ignoreKlass = [HEADLINES, SPOTLIGHT, NATION],
        includeKlass = None #[SPORT, POLITICS]
    )
