from nltk import PorterStemmer, FreqDist
from nltk.classify import apply_features
from nltk.classify.svm import SvmClassifier
from nltk.corpus import stopwords, LazyCorpusLoader, CategorizedPlaintextCorpusReader
from numpy.distutils.system_info import blas_mkl_info
import operator
from google_rss import HEADLINES, SPOTLIGHT, NATION
from logger import logger
from nltk import NaiveBayesClassifier, ConfusionMatrix
import random, nltk

__author__ = 'soldier'

import os

newsCorpus = LazyCorpusLoader('news_corpus', CategorizedPlaintextCorpusReader, '(data).*', cat_file='cats.txt')

class FeatureGenerator():

    def __init__(self, freqDists, featureFd):
        labels = freqDists.keys()
        featureToMaxRatio = {feature: 0 for feature in featureFd.samples()}
        for feature in featureFd.samples():
            featureRatio = []
            for fd1 in freqDists.values():
                featureFrequencyOutsideFd1 = 0
                for fd2 in freqDists.values():
                    if fd2 is not fd1:
                        featureFrequencyOutsideFd1 += fd2.freq(feature)
                featureRatio.append(fd1.freq(feature) / featureFrequencyOutsideFd1 if featureFrequencyOutsideFd1 != 0 else 0)
            featureToMaxRatio[feature] = max(featureRatio)
        bestFeatures = sorted(featureToMaxRatio.iteritems(), key=operator.itemgetter(1), reverse=True)
        logger.info(u"Best features: " + unicode(bestFeatures[:3000]))
        self.__features = [item[0] for item in bestFeatures[:3000]]


    def __call__(self, document):
        tokens = [t.lower() for t in nltk.wordpunct_tokenize(document)]
        doc = tokens#[tokens + nltk.bigrams(tokens)]
        return dict([("contains(%s)" % str(f), f in doc) for f in self.__features])

class NewsClassificator():

    def __init__(self, dir, doTest = True, ignoreKlass = []):
        logger.info("Start building " + self.__class__.__name__)
        self.__dir = dir
        self.__filenameToUrl = self.__readLogFile()
        self.__stemmer = PorterStemmer()
        freqDists = {}
        ignore = stopwords.words('english')
        featureFd = FreqDist()
        for klassId in self.__klasses(ignoreKlass):
            freqDist = FreqDist()
            for url, txt in self.__documents(klassId).items():
                txt = [ self.__stemmer.stem(token.lower()) for token in nltk.wordpunct_tokenize(txt)]
                for part in txt:
                    if part.isalnum() and part not in ignore:
                        freqDist.inc(part)
                        featureFd.inc(part)
                #for bigram in nltk.bigrams(txt):
                #    freqDist.inc(bigram)
                #    featureFd.inc(bigram)
            freqDists[klassId] = freqDist
        documentsWithLabel = [(document, label) for label in self.__klasses(ignoreKlass) for url, document in self.__documents(label).items()]
        random.shuffle(documentsWithLabel)
        self.__featuresGenerator = FeatureGenerator(freqDists, featureFd)
        testset = apply_features(self.__featuresGenerator, documentsWithLabel[:2000])
        trainset = apply_features(self.__featuresGenerator, documentsWithLabel)
        self.__classifier = NaiveBayesClassifier.train(trainset)
        logger.info(u"Classifier learned (set size=" + unicode(len(trainset)) + u")")
        if doTest:
            ref = [label for features, label in testset]
            test = [self.__classifier.classify(features) for features, cat in testset]
            logger.info("\n" + ConfusionMatrix(ref, test).pp())
            #logger.info("Accuracy: " + str(nltk.classify.accuracy(self.__classifier, testset)))
            self.__classifier.show_most_informative_features(n=30)

    def classify(self, document):
        return self.__classifier.classify(self.__featuresGenerator(document))

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

    def __documents(self, klassId):
        klassDir = os.path.join(self.__dir, klassId)
        results = {}
        for file in os.listdir(klassDir):
            if file.endswith(".txt"):
                fd = open(os.path.join(klassDir, file))
                url = self.__filenameToUrl[file]
                results[url] = fd.read()
                fd.close()
        return results

    def __klasses(self, ignoreKlass = []):
        results = []
        for dirEntry in os.listdir(self.__dir):
            if os.path.isdir(os.path.join(self.__dir, dirEntry)) and dirEntry not in ignoreKlass:
                results.append(dirEntry)
        return results

if __name__ == "__main__":
    nc = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/datasets/googlenews3/", doTest=True, ignoreKlass = [HEADLINES, SPOTLIGHT, NATION])
