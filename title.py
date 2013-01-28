# -*- coding: utf-8 *-*
from boilerpipe.extract import Extractor
from BeautifulSoup import BeautifulSoup
from logger import logger
import unicodedata as ud

def isIn(substring, text):
    substringPos = 0
    for c in text:
        if substringPos >= len(substring):
            return True
        l = substring[substringPos]
        if c == l:
            substringPos+=1
            continue
        else:
            cat1 = ud.category(c)
            cat2 = ud.category(l)
            #logger.info(c + u" " + cat1 + u" " + l + u" " + cat2)
            if cat1[0] == cat2[0] and cat1[0] in ["P"]:
                substringPos+=1
            else:
                substringPos = 0
    if substringPos == len(substring):
        return True
    return False


def isSubstr(find, data):
    if len(data) < 1 and len(find) < 1:
        return False
    for i in range(len(data)):
        if not isIn(find, data[i]):
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
    #logger.info(u"Results: " + u'\n-'.join(results))
    return results[0] if len(results) > 0 else ""

def __fixChars(text):
    return text.replace(unichr(160), " ").strip().replace("\n", " ").replace("\t", " ")



def fetchTitle(html, titles = None):
    if titles is None:
        titles = []
    bs = BeautifulSoup(html, convertEntities=BeautifulSoup.HTML_ENTITIES)
    metaTitle = bs.find("title")
    defaultTitle = ""
    ogMetaTitle = bs.find("meta", {"property": "og:title"})

    if ogMetaTitle:
        tagContent = __fixChars(ogMetaTitle["content"])
        return tagContent
    elif metaTitle:
        tagContent = __fixChars(u''.join(metaTitle.findAll(text=True)))
        defaultTitle = tagContent
        titles.append((defaultTitle, "meta"))
    #logger.info(u"Default title: " + unicode(defaultTitle))
    heads = []
    for i in [1, 2, 3, 4, 5]:
        tagsH =bs.findAll("h" + str(i))
        for tagH in tagsH:
            tagContent = __fixChars(u''.join(tagH.findAll(text=True)))
            titles.append((tagContent, "h"))
            heads.append(tagContent)
    #logger.info(u"Titles: " + unicode(titles))
    longestSubstring = longSubstrPair(titles)
    #logger.info(u"Longest substring: " + unicode(longestSubstring))
    longestSubstring = longestSubstring or defaultTitle
    title = longestSubstring
    title = __findHeader(heads, title)
    #obcinamy zbędne spacje
    while True:
        titleTmp = title.replace("  ", " ")
        if titleTmp == title:
            break
        title = titleTmp
    return title or defaultTitle

def __findHeader(heads, title):
    #szukamy nagłówka który zawiera tytuł
    results = {}
    for h in heads:
        l = len(longSubstr([title, h]))
        t = len(title)
        if isIn(h, title) and l <= t :
            results[h] = l
    if results:
        return sorted(results.items(), key=lambda x: x[1], reverse=True)[0][0]
    return title

def fetchTitleByUrl(url, titles=None):
    extractor = Extractor(extractor='ArticleExtractor', url=url)
    html =  extractor.data
    return fetchTitle(html, titles)

def __test(correctTitle, url):
    title = fetchTitleByUrl(url)
    if title.lower() != correctTitle.lower():
        logger.info(u"Wrong \"" + title + u"\" (\"" + correctTitle + u"\")")
        raise BaseException()
    else:
        logger.info(u"Correct: \"" + title + u"\"\n\n")

if __name__ == "__main__":
    logger.info(u"Test: " + longSubstr([u'Today\u2019s Christian Art \u2013 Some Useful Insights Revealed', u"Today's Christian Art - Some Useful Insights Revealed | Jesus Prince Of Peace by Akiane Kramarik"]))
    logger.info(u"Test: " + longSubstr([u'Adsense vs Amazon Associates vs iHerb vs Tradedoubler (review)', u"Adsense vs Amazon vs iHerb vs Tradedoubler (review) | Sidekick Blog"]))
    logger.info(longSubstr([u"tomek", u"tomek"]))
    logger.info(longSubstr([u"tom", u"tomek"]))
    logger.info(longSubstr([u"ek", u"tomek"]))
    logger.info(longSubstr([u"e", u"tomek"]))
    logger.info(longSubstr([u"test", u"tomek"]))
    #data = [u"'Striking Mom' Jessica Stilwell: blog extracts - Telegraph", u"'Striking Mom' Jessica Stilwell: blog extracts", u"Good on her! 'Striking Mom' Jessica Stilwell: blog extracts via @Telegraph http://t.co/I90LwcR6"]
    #print longSubstrPair(data)
    #data = [u"Public Speaking: Already a Decent Speaker? Here Are 5 Expert Tips | Inc.com", u"Already a Decent Speaker? Here Are 5 Expert Tips", u"RT @Inc: MT @avemii: a few #speakingintech tips from @Inc: http://t.co/yKH29bHq I think the last one—practicing a lot—may be the best tip"]
    #print longSubstrPair(data)
    #data = [u"BBC News - Abdominal aortic aneurysm screening rollout in Wales", u"Abdominal aortic aneurysm screening rollout in Wales", u"RT @bbchealth: Aneurysm scans rollout in Wales http://t.co/B0mjQwA6"]
    #print longSubstrPair(data)
    #data = [u"Threats and silence: the intimidation by Rangers fans | Alex Thomson's View", u"Threats and silence: the intimidation by Rangers fans", u"Alex Thomson's View", u"There are 151 comments on this post", u"Have your say", u"TOMOBLOG RANGERS INTIMIDATION"]
    #print longSubstrPair(data)

    #__test(u"We might be in the Scottish third division... but we're aiming for European super league, says Rangers chief",
    #    "http://www.dailymail.co.uk/sport/football/article-2216624/Rangers-join-European-super-league-says-Charles-Green.html?ITO=socialnet-twitter-mailfootball")
    #__test(u"Republic of Ireland 1-6 Germany", "http://www.bbc.co.uk/sport/0/football/19922447?utm_source=twitterfeed&utm_medium=twitter")
    #__test(u"Barnet 1-4 Plymouth", "http://www.bbc.co.uk/sport/0/football/19854907")
    #__test(u"Today's Christian Art - Some Useful Insights Revealed",
    #    "http://www.jesusprinceofpeace.com/blog/christian-art/todays-christian-art-insights-revealed")
    #__test(u"The Good, the Bad, and the Guilty: Anticipating Feelings of Guilt Predicts Ethical Behavior",
    #    "http://www.sciencedaily.com/releases/2012/10/121010141452.htm?utm_source=feedburner&utm_medium=feed&utm_campaign=Feed%3A+sciencedaily+%28ScienceDaily%3A+Latest+Science+News%29")
    #__test(u"Un-Bee-lievable! Dutch legend Edgar Davids is joining Barnet",
    #    "http://www.mirror.co.uk/sport/football/transfer-news/barnet-are-signing-edgar-davids-the-former-1374019")
    #__test(u"The Market Is Showing Signs Of Topping Out, And The Bad News Has Not Been Discounted",
    #    u"http://www.businessinsider.com/the-bad-news-has-not-been-discounted-2012-10?utm_source=feedburner&utm_medium=feed&utm_campaign=Feed%3A+businessinsider+%28Business+Insider%29")
    #__test(u"Mitt Romney, American Parasite",
    #    u"http://www.villagevoice.com/2012-04-18/news/Mitt-Romney-american-parasite/")
    #__test(u"CSR Racing", "https://itunes.apple.com/app/id469369175?mt=8")
    #__test(u"Romney's Convention Speech Destroyed: How Low Will He Go?", "http://explorer9360.xanga.com/767664210/romneys-convention-speech-destroyed-how-low-will-he-go/")
    __test(u"Funny as Hell – Kodi Me’chele Interview", u"http://www.playasonly.com/funny-as-hell-kodi-me%E2%80%99chele-interview/")
    __test(u"Apple Has Quietly Started Tracking iPhone Users Again, And It's Tricky To Opt Out", u"http://www.businessinsider.com/ifa-apples-iphone-tracking-in-ios-6-2012-10?op=1")
    __test(u"U-KISS’ Dongho to show his comedic side on ‘SNL Korea’", "http://www.allkpop.com/2012/10/u-kiss-dongho-to-show-his-comedic-side-on-snl-korea")
    __test(u"", u"")
    __test(u"Adsense vs Amazon Associates vs iHerb vs Tradedoubler (review)", u"http://pusabase.com/blog/2012/03/05/adsense-vs-amazon-associates-vs-iherb-vs-tradedoubler-review/")
    __test(u"", "http://thestar.blogs.com/thespin/2012/10/not-deja-vu-all-over-again.html")
    __test(u"", "http://www.france24.com/en/20121012-mars-rover-makes-surprising-rock-find?utm_source=dlvr.it&utm_medium=twitter")
    __test(u"", "http://globalgrind.com/news/russell-simmons-womens-rights-romney-obama-vote")
    __test(u"", "http://www.lancashiretelegraph.co.uk/sport/football/blackburn_rovers/news/9981760.Ewood_fears_as_manager_search_goes_on/?ref=twt")
    __test(u"", "http://buildmemuscle.wordpress.com/2012/10/11/great-leg-workout/")
