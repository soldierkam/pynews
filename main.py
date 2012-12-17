# -*- coding: utf-8 *-*
from nltk import FreqDist
import sys
from classifier import TxtClassificatorWrapper
from lang import TwitterLangDetect
from gui import Gui
from threading import Event
from Queue import Queue, Empty, Full, PriorityQueue
from save import Manager as StreamMgr
from wx.lib.pubsub.pub import Publisher
from logger import logger
from sql import SqlModel
from tools import StoppableThread, NothingToDo
from tweet import TweetText
from url import UrlResolverManager, UrlBuilder, UrlException
import os,cPickle
from user import UserBuilder
from user_tools import UserMgr
from web_server import EmbeddedHttpServer
from threading import Semaphore

tld = TwitterLangDetect()

class GetMsg(object):

    def __init__(self, cat):
        self.__mutex = Semaphore(0)
        self.__cat = cat

    def cat(self):
        return self.__cat

    def setResponse(self, res):
        self.__response = res
        self.__mutex.release()

    def getResponse(self):
        self.__mutex.acquire()
        return self.__response

class UserCrawler(StoppableThread):

    def __init__(self, token, userMgr, callback, userId=None, screenName=None):
        StoppableThread.__init__(self)
        self.__userMgr = userMgr
        self.__userId = userId
        self.__screenName = screenName
        self.__token = token
        self.__callback = callback

    def runPart(self):
        userFeatures = self.__userMgr.doJob(self.__token, self.__userId, self, self.__screenName)
        try:
            userFeatures.doJob()
            self.__callback(self, userFeatures)
        except NothingToDo as e:
            raise e
        except BaseException as e:
            logger.exception(u"UserFeatures.doJob error for " + unicode(self.__userId) + u" " + unicode(self.__screenName))
            raise e
        raise NothingToDo()

class CrawlerResult():

    def __init__(self, crawler, userFeatures):
        self.__uf = userFeatures
        self.__c = crawler

    def results(self):
        return self.__uf

    def crawler(self):
        return self.__c

class ResolvedTweetQueue(StoppableThread):

    def __init__(self, streamDir, userDir, userBuilder, urlBuilder):
        StoppableThread.__init__(self, self.__class__.__name__)
        self.__queue = PriorityQueue(maxsize=50)
        self.__urlBuilder = urlBuilder
        self.__userBuilder = userBuilder
        self.__dir = os.path.join(streamDir, "tweets")
        self.__model = SqlModel(os.path.join(streamDir, "finalUrl.db"), drop=False)
        self.__userMgr = UserMgr(userDir)
        self.__server = EmbeddedHttpServer(self, self.__userMgr)
        self.__crawlers = []

    def tweetResolved(self, tweet):
        self.__queue.put((10, tweet))

    def atBegin(self):
        self.__server.start()

    def getServerUrl(self):
        return self.__server.getHref()

    def runPart(self):
        try:
            obj = self.__queue.get(block=True, timeout=3)[1]
            if isinstance(obj, GetMsg):
                self.__parseGet(obj)
            elif isinstance(obj, CrawlerResult):
                self.__afterCallback(obj.crawler(), obj.results())
            else:
                self.__parseTweet(obj)

        except Empty:
            pass
        return

    def __parseGet(self, getMsg):
        urls = self.__model.selectUrls(getMsg.cat())
        urls = [u.copy() for u in urls]
        getMsg.setResponse(urls)

    def __parseTweet(self, tweet):
        for url in tweet.urls():
            if url.isError():
                logger.info(u"Tweet bad: wrong url: " + unicode(tweet) + u" " + unicode(url))
                self.__urlBuilder.delete(url)
                break
            url.setDocumentClasses(TxtClassificatorWrapper.instance().classify(url.getText()))
            if url.isRoot() or url.lang() != "en" or "short" in url.documentClasses():
                logger.info(u"Tweet bad: " + unicode(tweet) + u" " + unicode(url))
                self.__urlBuilder.delete(url)
                break
            logger.info(u"Tweet good: " + unicode(tweet) + u" " + unicode(url))
            logger.info(u"URL: " + unicode(url))
            self.__model.updateUrl(url)

    def __callback(self, crawler, userFeatures):
        self.__queue.put(CrawlerResult(crawler, userFeatures))

    def __createUserProfile(self):
        users = self.__model.selectUserWithoutCat(1)
        if not users:
            return False
        token = self.__server.getToken()
        for u in users:
            crawler = UserCrawler(token, self.__userMgr, self.__callback, userId=u.id)
            self.__crawlers.append(crawler)
        return True

    def __afterCallback(self, crawler, userFeatures):
        self.__crawlers.remove(crawler)
        userFeatures.cats()
        userFeatures.langs()

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__server.stop()
        self.__userMgr.close()

    def finalUrls(self, cat=None):
        req = GetMsg(cat)
        self.__queue.put((1, req))
        return req.getResponse()

    def __tweetWithUrlToRoot(self, tweet):
        for u in tweet.urls():
            if u.isRoot():
                return True
        return False

class Model(StoppableThread):

    def __init__(self, gui, stream, mainDir):
        StoppableThread.__init__(self, "Model")
        self.__iter = stream.__iter__()
        self.__elem = None
        self.__gui = gui
        self.__softPause = True
        self.__urlBuilder = UrlBuilder()
        self.__userBuilder = UserBuilder()
        streamDir=os.path.join(mainDir, "stream")
        userDir=os.path.join(mainDir, "user")
        self.__tweetResolvedListener = ResolvedTweetQueue(streamDir=streamDir, userDir=userDir, userBuilder=self.__userBuilder, urlBuilder=self.__urlBuilder)
        self.__urlResolver = UrlResolverManager(os.path.join(streamDir, "urlResolverCache.db2"), self.__tweetResolvedListener)
        self.__refreshGui = Event()
        self.__showProbDist = Event()
        self.__probDistUrl = None
        Publisher.subscribe(self.onPauseJob, "model.pause")
        Publisher.subscribe(self.onResumeJob, "model.start")
        Publisher.subscribe(self.onRefreshGui, "model.refreshGui")
        Publisher.subscribe(self.onProbDist, "model.prob_dist")
        Publisher.subscribe(self.onShowTreeMap, "model.showTreeMap")
        self.doPauseJob()
        self.start()

    def onRefreshGui(self, msg):
        self.__refreshGui.set()

    def onPauseJob(self, msg):
        d = msg.data
        if self.__softPause:
            self.__softPause = d["soft"] if d and "soft" in d else False
        self.doPauseJob()

    def onProbDist(self, msg):
        self.__showProbDist.set()
        self.__probDistUrl = msg.data

    def onShowTreeMap(self, msg):
        import webbrowser
        webbrowser.open(self.__tweetResolvedListener.getServerUrl())

    def doPauseJob(self):
        self.pauseJob()
        self.__urlResolver.pauseWorkers()
        Publisher.sendMessage("model.paused")

    def onResumeJob(self, msg):
        d = msg.data
        softResume = d["soft"] if d and "soft" in d else False
        userResume = not softResume
        if userResume or self.__softPause:
            logger.info("Continue job " + unicode(softResume) + unicode(self.__softPause))
            self.doContinueJob()
        else:
            logger.info("Ignore resume request")

    def doContinueJob(self):
        self.continueJob()
        self.__urlResolver.continueWorkers()
        Publisher.sendMessage("model.started")

    def atBegin(self):
        logger.info("Preparing model...")
        self.__urlResolver.start()
        logger.info("Start analyzing tweets")
        self.__tweetResolvedListener.start()

    def runPart(self):
        try:
            s = self.__elem or self.__iter.next()
            self.__elem = s
            if u'text' in s:
                try:
                    tweet = TweetText(s, self.__urlBuilder, self.__userBuilder)
                    for url in tweet.urls():
                        self.__urlResolver.addUrlToQueue(url)
                    retweeted = TweetText(s["retweeted_status"], self.__urlBuilder, self.__userBuilder) if "retweeted_status" in s else None
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

    def onPause(self):
        self._doSmthElse()

    def _doSmthElse(self):
        if self.__refreshGui.isSet():
            logger.info("Send data to GUI")
            self.__refreshGui.clear()
            data = {}
            data["urls"] = self.__tweetResolvedListener.finalUrls()
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
            probDistI = TxtClassificatorWrapper.instance().probDist(url.getText())

    def stop(self):
        StoppableThread.stop(self)
        self.__urlResolver.stop()
        self.__tweetResolvedListener.stop()

def main():
    mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews"
    tweetsDir = os.path.join(mainDir, "stream", "tweets")
    logger.info("Start app")
    model = None
    try:
        gui = Gui()
        mgr = StreamMgr(tweetsDir)
        model = Model(gui, stream=mgr.restore(lastOnly=True), mainDir=mainDir)
    except BaseException:
        logger.exception("Cannot start app")
        if model:
            model.stop()
        sys.exit(1)
        return
    gui.run()
    model.stop()
    logger.info("Exit app")

if __name__ == "__main__":
    main()
