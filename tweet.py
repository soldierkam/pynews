# -*- coding: utf-8 *-*
from geolocation import Position
from logger import logger
from url import UrlException

class TweetText:

    def __init__(self, t, urlBuilder, userBuilder, retweetedId):
        self.__id = t["id"]
        self.__inReplyToId = t["in_reply_to_status_id"]
        self.__retweetedId = retweetedId
        self.__retweets = t["retweet_count"]
        self.__text = t["text"]
        self.__coordinates = t["coordinates"]["coordinates"] if t["coordinates"] is not None and "coordinates" in t["coordinates"] else None
        self.__createdAt = t["created_at"]
        self.__urls = []
        self.__user = userBuilder.build(t["user"])
        self.__createUrl__(urlBuilder, t['entities']['urls'])
        if 'media' in t['entities']:
            self.__createUrl__(urlBuilder, t['entities']['media'])

    def __createUrl__(self, urlBuilder, entities):
        for urlEntity in entities:
            try:
                urlBuilder(self, urlEntity)
            except UrlException as ex:
                logger.info(unicode(ex))

    def addUrl(self, url):
        self.__urls.append(url)

    def urls(self):
        return self.__urls

    def retweets(self):
        return self.__retweets

    def location(self):
        if self.__coordinates:
            return Position(self.__coordinates[0], self.__coordinates[1])
        return None

    def createdAt(self):
        return self.__createdAt

    def dump(self):
        values = {}
        values["id"] = self.__id
        values["inReplyToId"] = self.__inReplyToId
        values["retweetedId"] = self.__retweetedId
        values["text"] = self.__text
        values["time"] = self.__createdAt
        values["retweets"] = self.__retweets
        values["user"] = self.__user.dump()
        values["coordinates"] = self.__coordinates
        return values

    def __eq__(self, other):
        return self.__id == other.__id

    def __hash__(self):
        return 3 + 7 * self.__id.__hash__()

    def text(self):
        return self.__text

    def user(self):
        return self.__user

    def id(self):
        return self.__id

    def inReplyToId(self):
        return self.__inReplyToId

    def retweetedId(self):
        return self.__retweetedId

    def isResolved(self):
        tweetResolved = True
        for url in self.urls():
            if not url.isResolvedOrError():
                tweetResolved = False
        return tweetResolved

    def __str__(self):
        return u"{TW: " + unicode(self.__text) + u" " + unicode(self.__id) + u"}"
