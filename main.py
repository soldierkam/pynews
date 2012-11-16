# -*- coding: utf-8 *-*
from nltk import FreqDist
from clustering import DocumentSizeClustering
from lang import TwitterLangDetect
from gui import Gui
from threading import Event
from Queue import Queue, Empty
from news import NewsClassificator
from save import Manager as StreamMgr
from wx.lib.pubsub.pub import Publisher
from logger import logger
from tools import StoppableThread, NothingToDo
from tweet import TweetText
from url import UrlResolverManager, UrlBuilder, UrlException
import os,cPickle, time

tld = TwitterLangDetect()

class TxtClassificatorWrapper():

    def __init__(self):
        self.__documentSizeClassificator = DocumentSizeClustering("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/datasets/extractedText.db")
        self.__newsClassificator = NewsClassificator("/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/datasets/googlenews/", doTest=False)
        #pass

    def classify(self, txt):
        #return []
        return self.__documentSizeClassificator.classify(txt), self.__newsClassificator.classify(txt)

class ResolvedTweetQueue(StoppableThread):

    def __init__(self, dir, classificator):
        StoppableThread.__init__(self, self.__class__.__name__)
        self.__queue = Queue()
        self.__classificator = classificator
        self.__tweets = []
        self.__dir = dir
        self.start()

    def tweetResolved(self, tweet):
        self.__queue.put(tweet)

    def runPart(self):
        try:
            tweet = self.__queue.get(block=True, timeout=3)
            url = tweet.urls()[0]
            if url.isError():
                logger.info(u"Tweet bad: wrong url: " + unicode(tweet) + u" " + unicode(url))
                return
            url.setDocumentClasses(self.__classificator.classify(url.getText()))
            if url.isRoot() or url.lang() != "en" or "short" in url.documentClasses():
                logger.info(u"Tweet bad: " + unicode(tweet) + u" " + unicode(url))
                return
            logger.info(u"Tweet good: " + unicode(tweet) + u" " + unicode(url))
            logger.info(u"URL: " + unicode(url))
            self.__tweets.append(tweet)
            if len(self.__tweets) % 100 == 99:
                self.__store()
            return
        except Empty:
            return

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__store()

    def __tweetWithUrlToRoot(self, tweet):
        for u in tweet.urls():
            if u.isRoot():
                return True
        return False

    def __store(self):
        file = os.path.join(self.__dir, "resolved_tweets.cpickle")
        os.remove(file)
        outputFile = open(file, "w")
        cPickle.dump(self.__tweets, outputFile)
        outputFile.close()

class Model(StoppableThread):

    def __init__(self, gui, stream, cacheDir):
        StoppableThread.__init__(self, "Model")
        self.__stream= stream
        self.__gui = gui;
        self.__urlFreq = FreqDist()
        self.__tweetResolvedListener = ResolvedTweetQueue(cacheDir, TxtClassificatorWrapper())
        self.__urlResolver = UrlResolverManager(os.path.join(cacheDir, "cache"), self.__tweetResolvedListener)
        self.__urlBuilder = UrlBuilder(self.__urlResolver, self.__urlFreq)
        self.__refreshGui = Event()
        Publisher.subscribe(self.onPauseJob, "model.pause")
        Publisher.subscribe(self.onResumeJob, "model.start")
        Publisher.subscribe(self.onRefreshGui, "model.refreshGui")
        self.doPauseJob()
        self.start()

    def onRefreshGui(self, msg):
        self.__refreshGui.set()

    def onPauseJob(self, msg):
        self.doPauseJob()

    def doPauseJob(self):
        StoppableThread.pauseJob(self)
        self.__urlBuilder.pauseResolver()
        Publisher.sendMessage("model.paused")

    def onResumeJob(self, msg):
        self.doContinueJob()

    def doContinueJob(self):
        StoppableThread.continueJob(self)
        self.__urlBuilder.resumeResolver()
        Publisher.sendMessage("model.started")

    def atBegin(self):
        logger.info("Preparing model...")
        self.__urlBuilder.init()
        logger.info("Start analyzing tweets")

    def runPart(self):
        iter = self.__stream.__iter__()
        for s in iter:
            if self.isStopping():
                logger.info("Model end")
                return False
            if self.isPaused():
                time.sleep(1)
                return

            if u'text' in s and s[u'retweet_count'] > 20:
                try:
                    tweet = TweetText(s, self.__urlBuilder)
                    retweeted = TweetText(s["retweeted_status"], self.__urlBuilder) if "retweeted_status" in s else None
                except UrlException as e:
                    logger.warn("Cannot build url: " + str(e))
                    continue
                #logger.info(unicode(tweet))
                #logger.info(unicode(retweeted))
                if self.__refreshGui.isSet():
                    self.__refreshGui.clear()
                    data = {}
                    data["urls"] = {sample: self.__urlFreq.freq(sample) for sample in set(self.__urlFreq.samples())}
                    data["cache"] = self.__urlResolver.cacheHitRate()
                    data["position"] = iter.position()
                    data["position_end"] = iter.count()
                    data["current_file_c"] = iter.currentFile()
                    data["last_file_c"] = iter.filesCount()
                    Publisher.sendMessage("update.urls", data=data)
        raise NothingToDo()

    def stop(self):
        StoppableThread.stop(self)
        self.__urlBuilder.stop()
        self.__tweetResolvedListener.stop()

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream"
    logger.info("Start app")
    gui = Gui()
    mgr = StreamMgr()
    model = Model(gui, stream=mgr.restore(mainDir), cacheDir=mainDir)
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
