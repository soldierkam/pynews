# -*- coding: utf-8 *-*

class User:

    def __init__(self, user):
        self.__timezone = user["time_zone"]
        self.__followersCount = user["followers_count"]
        self.__id = user["id"]
        self.__lang = user["lang"]
        self.__name = user["screen_name"]
        self.__createdAt = user["created_at"]
        self.__statusesCount = user["statuses_count"]

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

    def statusesCount(self):
        return self.__statusesCount

    def lang(self):
        return self.__lang

    def name(self):
        return self.__name

    def createdAt(self):
        return self.__createdAt

    def __repr__(self):
        return u"{User:%s,lang:%s,followers:%s,created_at:%s,statuses_count:%s}" % (unicode(self.__id), unicode(self.__lang), unicode(self.__followersCount), unicode(self.__createdAt), unicode(self.__statusesCount))

    def __eq__(self, other):
        if other is not User:
            return False
        return self.__id == other.__id

    def __hash__(self):
        return 3 + 7 * self.__id


class UserBuilder:

    def __init__(self, cache = {}):
        self.__users = cache

    def refreshCacheEntry(self, user):
        k = str(user.id())
        self.__users[k] = user

    def build(self, u):
        newUser = User(u)
        k = str(newUser.id())
        if newUser not in self.__users:
            self.__users[k] = newUser
        return self.__users[k]
