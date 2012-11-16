import urllib2
import simplejson
from threading import Lock
from logger import logger

class Position():

    def __init__(self, *args):
        self.__coordinates = map(str, args)
        self.__mutex = Lock()
        self.__results = None

    def __fetch(self):
        self.__mutex.acquire()
        try:
            if self.__results is None:
                url = "http://maps.googleapis.com/maps/api/geocode/json?latlng=" + ','.join(self.__coordinates) + "&sensor=false"
                json = simplejson.load(urllib2.urlopen(url))
                if json["status"] != "OK":
                    raise ValueError("Cannot get data for coordinates (" + ','.join(self.__coordinates) + "): " + str(json["status"]))
                self.__results = json["results"]
        finally:
            self.__mutex.release()


    def getCountry(self):
        self.__fetch()
        if len(self.__results) == 0:
            return None
        for addrComp in self.__results[0]["address_components"]:
            types = addrComp["types"]
            if "country" in types:
                return addrComp["short_name"], addrComp["long_name"]
        return None

if __name__ == "__main__":
    pos = Position(-75.14310264, 40.05701649)
    logger.info("Country: " + str(pos.getCountry()))

    pos = Position(51.890054,14.806275)
    logger.info("Country: " + str(pos.getCountry()))
