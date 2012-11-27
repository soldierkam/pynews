# -*- coding: utf-8 *-*
from nltk import FreqDist
from classifier import TxtClassificatorWrapper
from lang import TwitterLangDetect
from gui import Gui
from threading import Event
from Queue import Queue, Empty, Full
from save import Manager as StreamMgr
from wx.lib.pubsub.pub import Publisher
from logger import logger
from tools import StoppableThread, NothingToDo
from tweet import TweetText
from url import UrlResolverManager, UrlBuilder, UrlException
import os,cPickle

tld = TwitterLangDetect()



class ResolvedTweetQueue(StoppableThread):

    def __init__(self, dir, classificator):
        StoppableThread.__init__(self, self.__class__.__name__)
        self.__queue = Queue()
        self.__classificator = classificator
        self.__urls = []
        self.__dir = dir
        self.start()

    def tweetResolved(self, tweet):
        self.__queue.put(tweet)

    def runPart(self):
        try:
            tweet = self.__queue.get(block=True, timeout=3)
            for url in tweet.urls():
                if url.isError():
                    logger.info(u"Tweet bad: wrong url: " + unicode(tweet) + u" " + unicode(url))
                    break
                url.setDocumentClasses(self.__classificator.classify(url.getText()))
                if url.isRoot() or url.lang() != "en" or "short" in url.documentClasses():
                    logger.info(u"Tweet bad: " + unicode(tweet) + u" " + unicode(url))
                    break
                logger.info(u"Tweet good: " + unicode(tweet) + u" " + unicode(url))
                logger.info(u"URL: " + unicode(url))
                self.__urls.append(url)
                if len(self.__urls) % 100 == 99:
                    self.__store()
            return
        except Empty:
            return

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__store()

    def finalTweets(self):
        tweets = set()
        for url in self.__urls:
            for tweet in url.tweets():
                tweets.add(tweet)
        return tweets

    def finalUrls(self):
        return self.__urls

    def __tweetWithUrlToRoot(self, tweet):
        for u in tweet.urls():
            if u.isRoot():
                return True
        return False

    def __store(self):
        file = os.path.join(self.__dir, "resolved_tweets.cpickle")
        os.remove(file)
        outputFile = open(file, "w")
        cPickle.dump(self.finalTweets(), outputFile)
        outputFile.close()

class Model(StoppableThread):

    def __init__(self, gui, stream, cacheDir):
        StoppableThread.__init__(self, "Model")
        self.__iter = stream.__iter__()
        self.__elem = None
        self.__gui = gui;
        self.__urlFreq = FreqDist()
        self.__classificator = TxtClassificatorWrapper()
        self.__tweetResolvedListener = ResolvedTweetQueue(os.path.join(cacheDir, "tweets"), self.__classificator)
        self.__urlResolver = UrlResolverManager(os.path.join(cacheDir, "urlResolverCache.db"), self.__tweetResolvedListener)
        self.__urlBuilder = UrlBuilder(self.__urlFreq)
        self.__refreshGui = Event()
        self.__showProbDist = Event()
        self.__probDistUrl = None
        Publisher.subscribe(self.onPauseJob, "model.pause")
        Publisher.subscribe(self.onResumeJob, "model.start")
        Publisher.subscribe(self.onRefreshGui, "model.refreshGui")
        Publisher.subscribe(self.onProbDist, "model.prob_dist")
        self.doPauseJob()
        self.start()

    def onRefreshGui(self, msg):
        self.__refreshGui.set()

    def onPauseJob(self, msg):
        self.doPauseJob()

    def onProbDist(self, msg):
        self.__showProbDist.set()
        self.__probDistUrl = msg.data

    def doPauseJob(self):
        self.pauseJob()
        self.__urlResolver.pauseWorkers()
        Publisher.sendMessage("model.paused")

    def onResumeJob(self, msg):
        self.doContinueJob()

    def doContinueJob(self):
        self.continueJob()
        self.__urlResolver.continueWorkers()
        Publisher.sendMessage("model.started")

    def atBegin(self):
        logger.info("Preparing model...")
        self.__urlResolver.start()
        logger.info("Start analyzing tweets")

    def runPart(self):
        try:
            s = self.__elem or self.__iter.next()
            self.__elem = s
            if u'text' in s:
                try:
                    tweet = TweetText(s, self.__urlBuilder)
                    for url in tweet.urls():
                        self.__urlResolver.addUrlToQueue(url)
                    retweeted = TweetText(s["retweeted_status"], self.__urlBuilder) if "retweeted_status" in s else None
                    if retweeted:
                        for url in tweet.urls():
                            self.__urlResolver.addUrlToQueue(url)

                except UrlException as e:
                    logger.warn("Cannot build url: " + str(e))
            self._doSmthElse()
            self.__elem = None
        except Full:
            return
        except StopIteration:
            raise NothingToDo()

    def _doSmthElse(self):
        if self.__refreshGui.isSet():
            logger.info("Send data to GUI")
            self.__refreshGui.clear()
            data = {}
            data["urls"] = [(urlSample, self.__urlFreq.freq(urlSample), urlSample in self.__tweetResolvedListener.finalUrls()) for urlSample in set(self.__urlFreq.samples())]
            data["cache"] = self.__urlResolver.cacheHitRate()
            data["position"] = self.__iter.position()
            data["position_end"] = self.__iter.count()
            data["current_file_c"] = self.__iter.currentFile()
            data["last_file_c"] = self.__iter.filesCount()
            Publisher.sendMessage("update.urls", data=data)
        if self.__showProbDist.isSet():
            url = self.__probDistUrl
            self.__showProbDist.clear()
            self.__probDistUrl = None
            probDistI = self.__classificator.probDist(url.getText())

    def stop(self):
        StoppableThread.stop(self)
        self.__urlResolver.stop()
        self.__tweetResolvedListener.stop()

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream"
    tweetsDir = os.path.join(mainDir, "tweets")
    logger.info("Start app")
    gui = Gui()
    mgr = StreamMgr(tweetsDir)
    model = Model(gui, stream=mgr.restore(lastOnly=True), cacheDir=mainDir)
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
