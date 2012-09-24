# -*- coding: utf-8 *-*

from nltk.util import trigrams as nltk_trigrams
from nltk.tokenize import word_tokenize as nltk_word_tokenize
from nltk.probability import FreqDist
from nltk.corpus.util import LazyCorpusLoader
from nltk.corpus.reader.api import CorpusReader
from nltk.corpus.reader.util import StreamBackedCorpusView, concat
from math import log


#http://misja.posterous.com/language-detection-with-python-nltk

class LangIdCorpusReader(CorpusReader):
    '''
    LangID corpus reader
    '''
    CorpusView = StreamBackedCorpusView

    def _get_trigram_weight(self, line):
        '''
        Split a line in a trigram and its frequency count
        '''
        data = line.strip().split(' ')
        if len(data) == 2:
            return (data[1], int(data[0]))

    def _read_trigram_block(self, stream):
        '''
        Read a block of trigram frequencies
        '''
        freqs = []
        for i in range(20): # Read 20 lines at a time.
            freqs.append(self._get_trigram_weight(stream.readline()))
        return filter(lambda x: x != None, freqs)

    def freqs(self, fileids=None):
        '''
        Return trigram frequencies for a language from the corpus
        '''
        return concat([self.CorpusView(path, self._read_trigram_block) for path in self.abspaths(fileids=fileids)])

class LangDetect(object):
    language_trigrams = {}
    langid            = LazyCorpusLoader('langid', LangIdCorpusReader, r'(?!\.).*\.txt')

    def __init__(self, languages=['nl', 'en', 'fr', 'de', 'es']):
        for lang in languages:
            self.language_trigrams[lang] = FreqDist()
            for f in self.langid.freqs(fileids=lang+"-3grams.txt"):
                self.language_trigrams[lang].inc(f[0], f[1])

    def detect(self, text):
        '''
        Detect the text's language
        '''
        #print "Detect: " + text
        words    = nltk_word_tokenize(text.lower())
        trigrams = {}
        scores   = dict([(lang, 0) for lang in self.language_trigrams.keys()])

        for match in words:
            word_trigrams = self.get_word_trigrams(match)
            #print "Match: " + match
            #print "trigrams: " + str(word_trigrams)
            for trigram in word_trigrams:
                if not trigram in trigrams.keys():
                    trigrams[trigram] = 0
                trigrams[trigram] += 1

        total = sum(trigrams.values())

        for trigram, count in trigrams.items():
            for lang, frequencies in self.language_trigrams.items():
                # normalize and add to the total score
                scores[lang] += (float(frequencies[trigram]) / float(frequencies.N())) * (float(count) / float(total))

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        #print sorted_scores
        for lang, score in sorted_scores:
            if score > 0:
                return lang
        return None

    def get_word_trigrams(self, match):
        return [''.join(trigram) for trigram in nltk_trigrams(match) if trigram != None]

class TwitterLangDetect(LangDetect):

    def detect(self, tweet):
        txt = tweet['text']
        urls = tweet['entities']['urls']
        mentions = tweet['entities']['user_mentions']
        hashtags = tweet['entities']['hashtags']
        media = tweet['entities']['media'] if 'media' in tweet['entities'] else []
        indices = [];
        self.__appendIndices(indices, urls);
        self.__appendIndices(indices, mentions);
        self.__appendIndices(indices, hashtags);
        self.__appendIndices(indices, media);
        indices = sorted(indices)
        txt = self.__filterIndices(indices, txt);
        txt = self.__filterRT(txt)
        #print "Without entities: " + txt
        return super(TwitterLangDetect, self).detect(txt)

    def __appendIndices(self, indices, entities):
        for e in entities:
            indices.append(e['indices'])

    def __filterIndices(self, indices, txt):
        removed = 0
        for ind in indices:
            start = ind[0]-removed
            end = ind[1]-removed
            txt = txt[:start] + txt[end:]
            removed += ind[1] - ind[0]
        return " ".join(txt.split())

    def __filterRT(self, txt):
        return " ".join(txt.split("RT"))