# -*- coding: utf-8 *-*
import tweetstream
from lang import TwitterLangDetect
from my_collections import IncDict
import os

ld = TwitterLangDetect()

class TweetText:

    def __init__(self, t):
        self.__id = t["id"]
        self.__text = t["text"]
        self.__lang = ld.detect(t)

    def __eq__(self, other):
        return self.__id == other.__id

    def text(self):
        return self.__text

    def id(self):
        return self.__id

    def lang(self):
        return self.__text

    def __str__(self):
        return unicode(self.__id) + u":" + unicode(self.__text) + u"(" + unicode(self.__lang) + u")"

def main():
    stream = tweetstream.SampleStream(u'soldierkam', os.environ["PASSWORD"])
    url_counter = IncDict()
    i = 0
    for s in stream:
        if u'text' in s and s[u'retweet_count'] > 20:
            tweet = TweetText(s)
            retweeted = TweetText(s["retweeted_status"]) if "retweeted_status" in s else None
            print unicode(tweet)
            print unicode(retweeted)
            for url in s['entities']['urls']:
                url_counter.inc(url["url"])
            if i > 100:
                print url_counter
                i=0
            else:
                i+=1

if __name__ == "__main__":
    main()
