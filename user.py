class User:

    def __init__(self, user):
        self.__timezone = user["time_zone"]
        self.__followersCount = user["followers_count"]
        self.__id = user["id"]
        self.__lang = user["lang"]


    def __str__(self):
        return u"{User:" + unicode(self.__id) + u",lang:" + unicode(self.__lang) + u",followers:" + unicode(self.__followersCount) + u")"