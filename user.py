# -*- coding: utf-8 *-*

class User:

    def __init__(self, user):
        self.__timezone = user["time_zone"]
        self.__followersCount = user["followers_count"]
        self.__id = user["id"]
        self.__lang = user["lang"]

    def id(self):
        return self.__id

    def dump(self):
        values = {}
        values["timezone"] = self.__timezone
        values["followers_count"] = self.__followersCount
        values["id"] = self.__id
        values["lang"] = self.__lang
        return values

    def friendsCount(self):
        return self.__followersCount

    def __str__(self):
        return u"{User:" + unicode(self.__id) + u",lang:" + unicode(self.__lang) + u",followers:" + unicode(self.__followersCount) + u")"

