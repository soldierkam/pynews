import os
import re
import shelve
import threading
import thread
from time import sleep
import tweepy
from logger import logger

class OAuthClient():

    def __init__(self, consumer_key, consumer_secret):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        conf = self.__readSettings()
        if conf:
            auth.set_access_token(conf[0], conf[1])
        else:
            import pygtk
            pygtk.require('2.0')
            import gtk
            clipboard = gtk.clipboard_get()
            auth_url = auth.get_authorization_url()
            oldText = clipboard.wait_for_text()
            self.__openUrlInBrowser(auth_url)
            while True:
                verifier = clipboard.wait_for_text()
                if verifier is not None and oldText != verifier and re.compile(r"\d{7}").match(verifier):
                    logger.info("Pass verifier: " + str(verifier))
                    break;
                else:
                    clipboard.clear()
                    logger.info("wrong verifier: " + str(verifier))
                    sleep(1)
            auth.get_access_token(verifier)
        print "ACCESS_KEY = '%s'" % auth.access_token.key
        print "ACCESS_SECRET = '%s'" % auth.access_token.secret
        self.__auth = auth
        self.__writeSettings()

    def accessToken(self):
        return self.__auth.access_token.key, self.__auth.access_token.secret

    def auth(self):
        return self.__auth

    def __openUrlInBrowser(self, href):
        import webbrowser
        webbrowser.open(href)

    def __readSettings(self):
        dir = os.path.expanduser("~/.pynews")
        settingsFile = os.path.join(dir, "oauth")
        if not os.path.exists(settingsFile):
            return None
        map = shelve.open(settingsFile, "r")
        conf = map["consumer_key"], map["consumer_secret"]
        map.close()
        return conf

    def __writeSettings(self):
        dir = os.path.expanduser("~/.pynews")
        if not os.path.exists(dir):
            os.mkdir(dir)
        settingsFile = os.path.join(dir, "oauth")
        map = shelve.open(settingsFile, "c")
        map["consumer_key"] = self.__auth.access_token.key
        map["consumer_secret"] = self.__auth.access_token.secret
        map.close()

if __name__ == "__main__":
    oauthClient = OAuthClient(os.environ["consumer_key"], os.environ["consumer_secret"])