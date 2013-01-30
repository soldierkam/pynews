# -*- coding: utf-8 *-*
import os
from logger import logger
from save import Manager as StreamMgr

mainDir="/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/tweets-test"
mgr = StreamMgr(mainDir)
s = mgr.restore(lastOnly=False)
c = -1
for t in s:
    #logger.info(t)
    c += 1
    if "retweeted_status" in t and t[u"retweeted_status"][u"retweet_count"] > 0 and len(t[u"retweeted_status"][u"entities"][u"urls"]) > 0:
        urls = t[u"retweeted_status"][u"entities"][u"urls"]
        logger.info(str(t[u"retweeted_status"][u"retweet_count"]) + " " + str([url["expanded_url"] for url in urls if url["expanded_url"]]))
    if c % 1000 == 0:
        logger.info("Done: " + str(c))
