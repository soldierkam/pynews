from __builtin__ import tuple
import threading
from nltk import PorterStemmer, FreqDist
from nltk.classify import apply_features
from nltk.corpus import stopwords, LazyCorpusLoader, CategorizedPlaintextCorpusReader
import operator
from google_rss import HEADLINES, SPOTLIGHT, NATION, SPORT, POLITICS
from logger import logger
from nltk import NaiveBayesClassifier, ConfusionMatrix
import random, nltk
from tools import RssDataReader

newsCorpus = LazyCorpusLoader('news_corpus', CategorizedPlaintextCorpusReader, '(data).*', cat_file='cats.txt')
_stemmer = PorterStemmer()

def tokenize(txt):
    return [ _stemmer.stem(token.lower()) for token in nltk.wordpunct_tokenize(txt)]

class FeatureGenerator():

    def __init__(self, freqDists, features, klassSize):
        featureToMaxRatio = {feature: 0 for feature in features}
        #featureToAvgRatio = {feature: 0 for feature in featureFd.samples()}
        for feature in features:
            featureRatio = []
            for klass, fd1 in freqDists.items():
                frequency = fd1.freq(feature) * klassSize[klass]
                for fd2 in freqDists.values():
                    if fd2 is not fd1:
                        frequency2 = fd2.freq(feature) * klassSize[klass]
                        featureRatio.append( frequency / frequency2 if frequency2 > 0 else 0 )
            featureToMaxRatio[feature] = max(featureRatio)
            #featureToAvgRatio[feature] = sum(featureRatio)/ len(featureRatio)
            #featureToMaxRatio[feature] = sum(featureRatio) / len(featureRatio)
            #featureToMaxRatio[feature] = sorted(featureRatio, reverse=True)[2]
        bestMaxFeatures = sorted(featureToMaxRatio.iteritems(), key=operator.itemgetter(1), reverse=True)
        #klassDiscriminators = FreqDist()
        #for tokenWithFreq, klassDiscriminator in bestMaxFeatures[:3000]:
        #    klassDiscriminators.inc(klassDiscriminator[1])
        #logger.info("Features in each klass:")
        #klassDiscriminators.tabulate()

        #bestAvgFeatures = sorted(featureToAvgRatio.iteritems(), key=operator.itemgetter(1), reverse=True)
        logger.info(u"Best features: " + unicode(bestMaxFeatures[:3000]))
        self.__features = [item[0] for item in bestMaxFeatures[:3000]]# + [item[0] for item in bestAvgFeatures[:1500]]

    def __getFeatures(self, document):
        tokens = tokenize(document)
        doc = tokens#[tokens + nltk.bigrams(tokens)]
        return dict([(u"contains(%s)" % unicode(f), f in doc) for f in self.__features])

    def __call__(self, document):
        return self.__getFeatures(document)


class NewsClassificator(RssDataReader):

    def __init__(self, dir, testDir=None, doTest = True, ignoreKlass = [], includeKlass = None):
        RssDataReader.__init__(self, dir, testDir)
        logger.info("Start building " + self.__class__.__name__)
        self.__mutex = threading.Semaphore()

        freqDists = {}
        ignore = stopwords.words('english')
        features = set()
        klassSize = {}
        for klassId in self.klasses(ignoreKlass, includeKlass):
            freqDist = FreqDist()
            size = 0
            for url, txt in self.documents(klassId).items():
                txt = tokenize(txt)
                size += 1
                for part in txt:
                    if part.isalnum() and part not in ignore:
                        freqDist.inc(part)
                        features.add(part)
                #for bigram in nltk.bigrams(txt):
                #    freqDist.inc(bigram)
                #    featureFd.inc(bigram)
            freqDists[klassId] = freqDist
            klassSize[klassId] = size

        documentsWithLabel = [(document, correctKlass) for correctKlass in self.klasses(ignoreKlass, includeKlass) for url, document in self.documents(correctKlass).items()]
        random.shuffle(documentsWithLabel)

        self.__featuresGenerator = FeatureGenerator(freqDists, features, klassSize)
        trainset = apply_features(self.__featuresGenerator, documentsWithLabel)
        self.__classifier = NaiveBayesClassifier.train(trainset)
        logger.info(u"Classifier learned (set size=" + unicode(len(trainset)) + u")")
        if doTest:
            ref = []
            test = []
            testDocumentsWithLabel = [(document, correctKlass, url) for correctKlass in self.klasses(ignoreKlass, includeKlass) for url, document in self._testDocuments(correctKlass).items()]
            for doc, cat, url in testDocumentsWithLabel:
                ans = self.__classifier.classify(self.__featuresGenerator(doc))
                ref.append(cat)
                test.append(ans)
                if ans != cat:
                    logger.info(u"Wrong " + ans + u"(" + cat + u"):\t" + url + u" " + doc.replace("\n", " "))
            #for correctKlass, klass, featuresWithLabel in zip(ref, test, testset):
            #    if correctKlass != klass:
            #        pd = self.__classifier.prob_classify(dict(featuresWithLabel[0]))
            #        labelProbList = sorted( [(sample, pd.logprob(sample)) for sample in pd.samples()], key=lambda x: x[1], reverse=True)
            #        logger.info( correctKlass + " as " + klass + ": " + str([(correctKlass, "%.2f" % prob) for correctKlass, prob in labelProbList]))
            #        logger.info([(key, value)for key, value in featuresWithLabel[0].items() if value > 0])
            #        logger.info(self.__findDocumentByKlassAndFeatures(correctKlass, featuresWithLabel[0]))
            logger.info("\n" + ConfusionMatrix(ref, test).pp())
            #testset = apply_features(self.__featuresGenerator, testDocumentsWithLabel
            #logger.info("Accuracy: " + str(nltk.classify.accuracy(self.__classifier, testset)))
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


    def __findDocumentByKlassAndFeatures(self, klassId, features):
        docs = self._documents(klassId)
        for url, doc in docs.items():
            docFeatures = self.__featuresGenerator(doc)
            if docFeatures == features:
                return doc
        return None


if __name__ == "__main__":
    nc = NewsClassificator(
        #"/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews-all/",
        "/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/googlenews-26.01/",
        #testDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews-3.12/",
        #testDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/googlenews-all/",
        doTest=True,
        ignoreKlass = [HEADLINES, SPOTLIGHT, NATION],
        includeKlass = None #[SPORT, POLITICS]
    )
