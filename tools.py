# -*- coding: utf-8 *-*

from datetime import datetime
from threading import Thread, Event
from boilerpipe.extract import Extractor
from logger import logger
import time
from hashlib import sha1
from BeautifulSoup import BeautifulSoup

class RateMonitor():

    def __init__(self):
        self.__bucket = {}
        self.__sec = -1
        for i in range(0, 60):
            self.__bucket[i] = 0
        self.__rounds = 0
        self.__i = 0
        self.__start = datetime.now()

    def hit(self, c=1):
        sec = datetime.now().second
        if sec != self.__sec:
            self.__bucket[sec] = 0
            self.__sec = sec
            self.__rounds += 1
        self.__bucket[sec] = self.__bucket[sec] + c
        self.__i += 1

    def value(self):
        sec = datetime.now().second
        assert sec >=0 and sec <=59
        return self.__bucket[sec]

    def lastAvg(self):
        if self.__rounds < len(self.__bucket):
            return None
        s = 0.0
        for v in self.__bucket.values():
            s += v
        return round(s / 60, 2)

    def avg(self):
        diff = datetime.now() - self.__start
        return self.__i / diff.seconds

class NothingToDo(Exception):

    def __init(self):
        pass

class StoppableThread(Thread):

    def __init__(self, name = None):
        name = name or self.__class__.__name__
        Thread.__init__(self, name=name)
        self.__stop = Event()
        self.error = Event()
        self.__pauseEvent = Event()
        self.__msgCount = 0

    def stop(self):
        self.__stop.set()

    def isStopping(self):
        return self.__stop.isSet()

    def pauseJob(self):
        self.__msgCount = 0
        self.__pauseEvent.set()

    def continueJob(self):
        self.__pauseEvent.clear()

    def isPaused(self):
        return self.__pauseEvent.isSet()

    def run(self):
        try:
            self.atBegin()
            while not self.__stop.isSet():
                try:
                    #logger.debug("Check is paused?")
                    if self.__pauseEvent.isSet():
                        self.onPause()
                        time.sleep(1)
                        continue
                    #logger.debug("Run part...")
                    self.runPart()
                except NothingToDo:
                    logger.info("nothing to do")
                    break
                except BaseException as exc:
                    self.error.set()
                    logger.exception("error")
                    break
            self.atEnd()
        except:
            logger.exception("Fatal error")

    def onPause(self):
        if self.__msgCount % 60 == 0:
            logger.debug("Paused")
        self.__msgCount += 1
        self.__msgCount %= 60

    def runPart(self):
        pass

    def atBegin(self):
        pass

    def atEnd(self):
        logger.info("exiting...")
        pass

def stringToDigest(string):
    return sha1(string).hexdigest()

def isSubstr(find, data):
    if len(data) < 1 and len(find) < 1:
        return False
    for i in range(len(data)):
        if find not in data[i]:
            return False
    return True

def longSubstr(data):
    substr = ''
    if len(data) > 1 and len(data[0]) > 0:
        for i in range(len(data[0])):
            for j in range(len(data[0])-i+1):
                if j > len(substr) and isSubstr(data[0][i:i+j], data):
                    substr = data[0][i:i+j]
    return substr

def longSubstrPair(data):
    #logger.info(u"Data: " + u'\n-'.join([txt + "//" + type for txt, type in data]))
    results = []
    for elem1 in data:
        for elem2 in data:
            if elem1 is not elem2 and elem1[1] != elem2[1]:
                substr = longSubstr([elem1[0], elem2[0]])
                #logger.info(u"\n\"" + unicode(elem1) + u"\"\n\"" + unicode(elem2) + u"\"\nis:\n\"" + unicode(substr) + u"\"")
                if len(substr) > 10:
                    results.append(substr)
    results = sorted(results, key = lambda x: len(x), reverse=True)
    #logger.info(results)
    #logger.info(u"Results: " + u'\n-'.join(results))
    return results[0] if len(results) > 0 else ""

def fetchTitle(html, titles = []):
    bs = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES)
    metaTitle = bs.find("title")
    defaultTitle = ""
    if metaTitle:
        tagContent = u''.join(metaTitle.findAll(text=True))
        defaultTitle = tagContent.strip()
        titles.append((defaultTitle, "meta"))
    heads = []
    for i in [1, 2, 3, 4, 5]:
        tagsH =bs.findAll("h" + str(i))
        for tagH in tagsH:
            tagContent = u''.join(tagH.findAll(text=True))
            titles.append((tagContent.strip(), "h"))
            heads.append(tagContent.strip())
    longestSubstring = longSubstrPair(titles).strip().replace("\n", " ").replace("\t", " ")
    #logger.info(u"Title: " + unicode(title))
    longestSubstring = longestSubstring or defaultTitle
    title = longestSubstring
    #szukamy nagłówka który zawiera tytuł
    titleTmp = title
    for h in heads:
        if title in h and len(longSubstr([title, h])) >= len(titleTmp) :
            titleTmp = h
    title = titleTmp
    #obcinamy zbędne spacje
    while True:
        titleTmp = title.replace("  ", " ")
        if titleTmp == title:
            break
        title = titleTmp
    return title

def fetchTitleByUrl(url, titles=[]):
    extractor = Extractor(extractor='ArticleExtractor', url=url)
    html =  extractor.data
    return fetchTitle(html, titles)

if __name__ == "__main__":
    #data = [u"'Striking Mom' Jessica Stilwell: blog extracts - Telegraph", u"'Striking Mom' Jessica Stilwell: blog extracts", u"Good on her! 'Striking Mom' Jessica Stilwell: blog extracts via @Telegraph http://t.co/I90LwcR6"]
    #print longSubstrPair(data)
    #data = [u"Public Speaking: Already a Decent Speaker? Here Are 5 Expert Tips | Inc.com", u"Already a Decent Speaker? Here Are 5 Expert Tips", u"RT @Inc: MT @avemii: a few #speakingintech tips from @Inc: http://t.co/yKH29bHq I think the last one—practicing a lot—may be the best tip"]
    #print longSubstrPair(data)
    #data = [u"BBC News - Abdominal aortic aneurysm screening rollout in Wales", u"Abdominal aortic aneurysm screening rollout in Wales", u"RT @bbchealth: Aneurysm scans rollout in Wales http://t.co/B0mjQwA6"]
    #print longSubstrPair(data)
    #data = [u"Threats and silence: the intimidation by Rangers fans | Alex Thomson's View", u"Threats and silence: the intimidation by Rangers fans", u"Alex Thomson's View", u"There are 151 comments on this post", u"Have your say", u"TOMOBLOG RANGERS INTIMIDATION"]
    #print longSubstrPair(data)
    #logger.info(fetchTitleByUrl("http://explorer9360.xanga.com/767664210/romneys-convention-speech-destroyed-how-low-will-he-go/"))
    #logger.info(fetchTitleByUrl("http://www.allkpop.com/2012/10/u-kiss-dongho-to-show-his-comedic-side-on-snl-korea"))
    #logger.info(fetchTitleByUrl("http://thestar.blogs.com/thespin/2012/10/not-deja-vu-all-over-again.html"))
    #logger.info(fetchTitleByUrl("http://www.france24.com/en/20121012-mars-rover-makes-surprising-rock-find?utm_source=dlvr.it&utm_medium=twitter"))
    logger.info(fetchTitleByUrl("http://globalgrind.com/news/russell-simmons-womens-rights-romney-obama-vote"))