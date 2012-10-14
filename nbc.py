__author__ = 'soldier'


from nltk.corpus.reader.plaintext import CategorizedPlaintextCorpusReader
from nltk import FreqDist
from nltk.classify.naivebayes import NaiveBayesClassifier
from logger import  logger
from nltk.corpus import LazyCorpusLoader
import nltk, random

my_corpus = LazyCorpusLoader(
    'my_corpus', CategorizedPlaintextCorpusReader, '(data).*',
    cat_file='cats.txt')

all_words = FreqDist(w.lower() for w in my_corpus.words())
word_features = all_words.keys()[:5000]

def document_features(document):
    document_words = set(document)
    features = {}
    for word in word_features:
        features['contains(%s)' % word] = (word in document_words)
    features["len"] = len(document_words)
    return features

def categoryMapper(category):
    if category in ("news", "article"):
        return "show"
    else:
        return "hide"

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/corpus2"
    input="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets/cache"
    logger.info("Start app")
    documents = [(list(my_corpus.words(fileid)), categoryMapper(category))
                 for category in my_corpus.categories()
                 for fileid in my_corpus.fileids(category)]
    random.shuffle(documents)
    featuresets = [(document_features(d), c) for (d,c) in documents]
    train_set, test_set = featuresets[150:], featuresets[:150]
    clf = NaiveBayesClassifier.train(train_set)
    logger.info("Accuracy: " + str(nltk.classify.accuracy(clf, test_set)))
    logger.info(clf.show_most_informative_features())
    logger.info("Exit app")

if __name__ == "__main__":
    main()