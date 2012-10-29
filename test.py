# -*- coding: utf-8 *-*
from nltk import cluster
from nltk.cluster import cosine_distance, euclidean_distance
from numpy import array
import shelve
import re
import urlparse
from nltk.corpus import stopwords
import codecs
from lang import LangDetect

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

langDetect = LangDetect()

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

filename = "/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets/cache"
data = shelve.open(filename);
#words = getWords(data)
#print "Words: " + str(len(words))
vectors = [features(item["text"]) for digest, item in data.items() if item["text"] and item["text"] != "ERROR" and langDetect.detect(item["text"]) is "en"]
clusterer = cluster.KMeansClusterer(3, euclidean_distance, initial_means=[[10,40,0,1],[30,500,0,30],[120,1500,15,50]])
#clusterer = cluster.GAAClusterer(2)
#clusterer = cluster.EMClusterer([[10, 10, 10, 10, 10, 10],[3, 3, 3, 3, 3, 3]], bias=0.1, normalise=True)
clusterer.cluster(vectors)

f1 = codecs.open('/tmp/workfile1', 'w', encoding="UTF-8")
f2 = codecs.open('/tmp/workfile2', 'w', encoding="UTF-8")
f3 = codecs.open('/tmp/workfile3', 'w', encoding="UTF-8")
# classify a new vector
for item in data.itervalues():
    text = item["text"]
    klassToWordsCount = {"1": 0, "2": 0, "3": 0}
    if text and text != "ERROR":
        feat = features(text)
        klass = str(clusterer.classify(feat))
        print text + " => " + klass
        if klass == "1":
            f = f1
        elif klass == "2":
            f = f2
        else:
            f = f3
        f.write("\n###### " + klass + " ####### " + item["url"] + str(feat) + " \n")
        f.write(text)
