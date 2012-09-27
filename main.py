# -*- coding: utf-8 *-*
import tweetstream
from tweetstream import ConnectionError
from lang import TwitterLangDetect
from my_collections import IncDict
import os
import sys
from gui import Gui
from threading import Thread
from boilerpipe.extract import Extractor
from Queue import Queue, Empty

ld = TwitterLangDetect()

TYPE_MEDIA = u"media"
TYPE_URL = u"url"

class TweetText:

    def __init__(self, t, url_counter, url_resolver):
        self.__id = t["id"]
        self.__text = t["text"]
        self.__lang = ld.detect(t)
        self.__url = None
        self.__url_counter = url_counter
        self.__url_resolver = url_resolver
        self.__createUrl__(t['entities']['urls'], TYPE_URL)
        if 'media' in t['entities']:
            self.__createUrl__(t['entities']['media'], TYPE_MEDIA)

    def __createUrl__(self, entities, type):
        for urlEntity in entities:
            self.__url = Url(urlEntity, type)
            self.__url, urlExisted = self.__url_counter.inc(self.__url)
            if not urlExisted:
                self.__url_resolver.addUrlToQueue(self.__url)

    def __eq__(self, other):
        return self.__id == other.__id

    def __hash__(self):
        return 3 + 7 * self.__id.__hash__()

    def text(self):
        return self.__text

    def id(self):
        return self.__id

    def lang(self):
        return self.__text

    def __str__(self):
        return unicode(self.__id) + u":" + unicode(self.__text) + u"(" + unicode(self.__lang) + u")"

class Url:

    def __init__(self, entity, type):
        self.__url = entity["url"]
        self.__expanded = entity["expanded_url"]
        self.__text = None
        self.__type = type

    def __eq__(self, other):
        if other:
            return self.__url == other.__url
        else:
            return False

    def __hash__(self):
        return 3 + 7 * self.__url.__hash__()

    def getText(self):
        return self.__text

    def setText(self, text):
        print "Url " + self.__url + " resolved"
        self.__text = text

    def getUrl(self):
        return self.__url

    def getExpandedUrl(self):
        return self.__expanded

    def isResolved(self):
        return True if self.__text else False

    def __unicode__(self):
        return u"{URL " + self.__type + u" :" + self.__url + u"}"

    def __str__(self):
        return self.__unicode__()

class UrlResolverWorker(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.__queue = queue
        self.__stop = False

    def run(self):
        while True:
            url = None
            while url == None:
                try:
                    url = self.__queue.get(True, 3)
                except Empty:
                    if self.__stop:
                        break

            if url == None:
                print "Worker end"
                return

            tryNumber = 0
            while tryNumber < 3:
                try:
                    if self.__stop:
                        return
                    extractor = Extractor(extractor='ArticleExtractor', url=url.getUrl())
                    url.setText(extractor.getText())
                    break
                except Exception as e:
                    print "Cannot resolve url " + unicode(url)
                    tryNumber += 1
            url = None

    def stop(self):
        self.__stop = True

class UrlResolverManager():

    def __init__(self):
        self.__queue = Queue()
        self.__workers = []
        for i in range(0,3):
            self.__workers.append(UrlResolverWorker(self.__queue))

    def start(self):
        for worker in self.__workers:
            worker.start()

    def stop(self):
        for worker in self.__workers:
            worker.stop()

    def addUrlToQueue(self, url):
        self.__queue.put(url)
        s = self.__queue.qsize()
        if s > 20:
            print "Queue size is too big: " + unicode(s)

class Model(Thread):

    def __init__(self, gui):
        Thread.__init__(self)
        self.__stream = tweetstream.SampleStream(u'soldierkam', os.environ["PASSWORD"])
        self.__url_counter = IncDict()
        self.__url_resolver = UrlResolverManager()
        self.__gui = gui;
        self.__stop = False
        self.start()

    def run(self):
        i = 0
        self.__url_resolver.start()
        connectionCounter=0;
        try:
            while connectionCounter < 7:
                for s in self.__stream:
                    if self.__stop:
                        print "Model end"
                        return
                    connectionCounter = 0
                    if u'text' in s and s[u'retweet_count'] > 20:
                        tweet = TweetText(s, self.__url_counter, self.__url_resolver)
                        retweeted = TweetText(s["retweeted_status"], self.__url_counter, self.__url_resolver) if "retweeted_status" in s else None
                        print unicode(tweet)
                        print unicode(retweeted)
                        if i > 10:
                            print unicode(self.__url_counter)
                            self.__gui.updateUrls(self.__url_counter)
                            i=0
                        else:
                            i+=1
                return
        except ConnectionError as err:
            print u"Connection error: " + unicode(err)
            connectionCounter += 1

    def stop(self):
        self.__stop = True
        self.__url_resolver.stop()

def main():
    gui = Gui()
    model = Model(gui)
    gui.run()
    model.stop()

if __name__ == "__main__":
    main()
