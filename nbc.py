

from nltk.corpus.reader.plaintext import CategorizedPlaintextCorpusReader
from nltk import FreqDist, BigramCollocationFinder, BigramAssocMeasures, TrigramCollocationFinder, TrigramAssocMeasures
from nltk.classify.naivebayes import NaiveBayesClassifier
from logger import  logger
from nltk.corpus import LazyCorpusLoader
from nltk.text import TextCollection
import nltk, random, operator, itertools

my_corpus = LazyCorpusLoader(
    'my_corpus', CategorizedPlaintextCorpusReader, '(data).*',
    cat_file='cats.txt')
stopwords = nltk.corpus.stopwords.words()

all_words = FreqDist(w.lower() for w in my_corpus.words() if w not in stopwords and len(w) > 2)
#all_words_inf = {}
#textCollection = TextCollection(my_corpus)
#for word in all_words.keys()[:1000]:
#    score = 0
#    for fileid in my_corpus.fileids():
#        text = my_corpus.raw(fileid)
#        score += textCollection.tf_idf(word, text)
#    all_words_inf[word] = score
#all_words = sorted(all_words_inf.items(), key=operator.itemgetter(1), reverse=False)
word_features = [word for word in all_words.keys() if len(word) > 2][:2000]


def document_features(words_in_document, score_fn=TrigramAssocMeasures.chi_sq, n=300):
    trigram_finder = TrigramCollocationFinder.from_words(words_in_document)
    trigrams = trigram_finder.nbest(score_fn, n)


    document_words = set(words_in_document)
    features = dict([(ngram, True) for ngram in itertools.chain(words_in_document, trigrams) if len(ngram) >= 2 and ngram not in stopwords])

    #for word in word_features:
    #    features['contains(%s)' % word] = (word in document_words)
    #    #features['count(%s)' % word] = (document.count(word))
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
    documents = [(list(w.lower() for w in my_corpus.words(fileid)), categoryMapper(category))
                 for category in my_corpus.categories()
                 for fileid in my_corpus.fileids(category)]
    random.shuffle(documents)

    featuresets = [(document_features(d), c) for (d,c) in documents]
    train_set, test_set = featuresets[250:], featuresets[:50]
    clf = NaiveBayesClassifier.train(train_set)
    logger.info("Accuracy: " + str(nltk.classify.accuracy(clf, test_set)))
    ref = [cat for features, cat in test_set]
    test = [clf.classify(features) for features, cat in test_set]
    logger.info(clf.show_most_informative_features(20))
    logger.info("\n" + nltk.ConfusionMatrix(ref, test).pp())
    logger.info("Exit app")

if __name__ == "__main__":
    main()