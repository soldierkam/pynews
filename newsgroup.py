import re, os
from nltk.stem.porter import PorterStemmer
from nltk import FreqDist

metaPattern = re.compile(r"([a-zA-Z\-]+): (.*)")

HEADER = 1
BODY = 2
FOOTER = 3

newsgroupClassToMyClass = {"talk.religion.misc": "religion",
                           "talk.politics.misc": "politics",
                           "talk.politics.mideast": "politics",
                           "talk.politics.guns": "politics",
                           "soc.religion.christian": "religion",
                           "sci.space": "science",
                           "sci.med": "science",
                           "sci.electronics": "science",
                           "sci.crypt": "science",
                           "rec.sport.hockey": "sport",
                           "rec.sport.baseball": "sport",
                           "rec.motorcycles": "motorization",
                           "rec.autos": "motorization",
                           "misc.forsale": "announce",
                           "comp.windows.x": "computers",
                           "comp.sys.mac.hardware": "computers",
                           "comp.sys.ibm.pc.hardware": "computers",
                           "comp.os.ms-windows.misc": "computers",
                           "comp.graphics": "computers",
                           "alt.atheism": "religion"}

class NewsPost():

    stemmer = PorterStemmer()

    def __init__(self, filename, klass):
        self.__klass = newsgroupClassToMyClass[klass]
        self.__text = ""
        self.__footer = ""
        self.__subject = ""
        part = None
        f = open(filename, "r")
        while True:
            lineText = f.readline()
            if lineText == "":
                break
            lineText = lineText[:-1]
            if part == None or part == HEADER:
                m = metaPattern.match(lineText)
                if m:
                    key = m.group(1)
                    value = m.group(2)
                    if key == "Subject":
                        self.__subject = value
                elif lineText == "":
                    part = BODY
            elif part == BODY:
                if lineText == "-- ":
                    part = FOOTER
                else:
                    self.__text += "\n" + lineText
            elif part == FOOTER:
                self.__footer += "\n" + lineText

    def __str__(self):
        return "NewsPost-%s{%s: %s }" % (self.__klass, self.__subject, self.__text[:100] + ("..." if len(self.__text) > 100 else ""))

    def klass(self):
        return self.__klass

    def stems(self):
        r = []
        for word in self.__text.split():
            if word.isalpha():
                r.append(NewsPost.stemmer.stem(word.lower()))
        return r

class NewsgroupUtils():

    def __init__(self, dir = "/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/newsgroup/"):
        trainData = os.path.join(dir, "20news-bydate-train")
        testData = os.path.join(dir, "20news-bydate-test")
        self.__klassList = set(newsgroupClassToMyClass.values())
        self.__freq = {}
        for k in self.__klassList:
            self.__freq[k] = FreqDist()
        self.__data = self.__appendNewsPost(self.__appendNewsPost([], trainData), testData)

    def __appendNewsPost(self, collection, dataDir):
        for klassId in os.listdir(dataDir):
            klassDir = os.path.join(dataDir, klassId)
            for filename in os.listdir(klassDir):
                np = NewsPost(os.path.join(klassDir, filename), klassId)
                freqDistKlass = self.__freq[np.klass()]
                for stem in np.stems():
                    freqDistKlass.inc(stem)
                collection.append(np)
        return collection

    def freq(self):
        return self.__freq

if __name__ == "__main__":
    nu = NewsgroupUtils()
    for klassId, freqDist in nu.freq().items():
        stems = freqDist.keys()
        for otherKlassId, otherFreqDist in nu.freq().items():
            if otherKlassId != klassId:
                otherStems = otherFreqDist.keys()
                for otherKlassStem in otherStems:
                    if otherKlassStem in stems:
                        stems.remove(otherKlassStem)
        print "Klass " + klassId + ": " + str(stems[:100])
