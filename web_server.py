# -*- coding: utf-8 *-*
from BaseHTTPServer import BaseHTTPRequestHandler
import SocketServer
import base64
import cgi
import os
import random
import shelve
from threading import Semaphore
import Cookie
import urlparse
import signal
import datetime
import simplejson
import tweepy
from tweepy import TweepError
from logger import logger
from tools import StoppableThread, NothingToDo

label2code = {
    "WORLD": "w",
    "BUSINESS": "b",
    "NATION": "n",
    "TECHNOLOGY": "tc",
    "SCIENCE": "snc",
    "ELECTIONS": "el",
    "POLITICS": "p",
    "ENTERTAINMENT": "e",
    "SPORT": "s",
    "HEALTH": "m",
    "HEADLINES": "h",
    "SPOTLIGHT": "ir",
    "ALL": None
}

code2label = {value: key for key, value in label2code.items()}

ext2mime = {
    ".html": "text/html",
    ".htm": "text/html",
    ".css": "text/css",
    ".js": "text/javascript",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".png": "image/png",
}

BLACK = u"#000000"
GRAY = u"#444444"

def _buildApiUsingSession(session):
    auth = tweepy.OAuthHandler(os.environ["consumer_key"], os.environ["consumer_secret"], callback="http://localhost:8888/callback.html", secure=True)
    auth.set_access_token(session['token'].key, session["token"].secret)
    return tweepy.API(auth)

class PynewsHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        self.__lazyHeaders = []
        BaseHTTPRequestHandler.__init__(self, request, client_address, server)

    def __buildData(self, label):
        cat = label2code.get(label)
        selectedUrls = self.server.urls(cat)
        return selectedUrls

    def __buildUserData(self, screenName=None):
        session = self.server.getSession(self)
        logged = "user" in session
        if screenName:
            screenName = screenName
        elif logged:
            screenName = session["user"].screen_name
        else:
            screenName = None

        if not screenName or not logged:
            return None

        sessionKey = "features-" + screenName
        if sessionKey not in session:
            logger.info("missing data for " + str(screenName))
            self.server.runDataCrawler(session, userId=None, screenName=screenName)
            return None
        userFeatures = session[sessionKey]
        return userFeatures.dump()

    def do_GET(self):
        (self.scm, self.netloc, self.path, self.params, self.query, self.fragment) = urlparse.urlparse(self.path, 'http')
        self.params = urlparse.parse_qs(self.query)
        self.__processRequest()

    def do_POST(self):
        (self.scm, self.netloc, self.path, self.params, self.query, self.fragment) = urlparse.urlparse(self.path, 'http')
        ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
        if ctype == 'multipart/form-data':
            self.params = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length = int(self.headers.getheader('content-length'))
            self.params = urlparse.parse_qs(self.rfile.read(length), keep_blank_values=1)
        else:
            self.params = {}
        self.__processRequest()

    def __processRequest(self):
        self.netloc = self.headers.getheader("host", default=None)
        methodName = self.generateMethodName()
        method = getattr(self, methodName, None)
        if method:
            method()
        else:
            if self.path.startswith("/static/"):
                self.GET_static()
            else:
                logger.warn("Cannot find method " + methodName)
                self.send_response(404)

    def generateMethodName(self):
        simplePath = ""
        drop = False
        for c in self.path:
            if str.isalnum(c):
                if drop:
                    c = str.upper(c)
                    drop = False
                simplePath += c
            else:
                drop = True
        return self.command + "_" + simplePath

    def GET_NewsmapJson(self):
        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        d = self.__buildData(self.params["c"][0] if "c" in self.params else "ALL")
        u = self.__buildUserData(self.params["screenName"][0] if "screenName" in self.params else None)
        dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None
        self.wfile.write(simplejson.dumps({"data": d, "user": u}, default=dthandler))

    def POST_SettingsJson(self):
        self.send_response(200, "OK")
        session = self.server.getSession(self)
        who = session["user"].screen_name if "user" in session else None
        if who:
            autorefresh = self.params["autorefresh"][0] if "autorefresh" in self.params else None
            screenName = self.params["screenName"][0] if "screenName" in self.params else None
            autorefresh = True if autorefresh == "true" else False
            if autorefresh and screenName:
                self.server.saveSettings(who, {"autorefresh": autorefresh, "screenName": screenName})
            else:
                logger.warning(u"Missing param: " + unicode(self.params))

    def GET_SettingsJson(self):
        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/json")
        session = self.server.getSession(self)
        self.end_headers()
        if "user" in session:
            user = session["user"]
            who = user.screen_name
            settings = self.server.readSettings(who, {"autorefresh": False, "screenName": who})
            data = {"is": True, "name": who, "settings": settings}
        else:
            data = {"is": False}
        self.wfile.write(simplejson.dumps(data))

    def GET_CallbackHtml(self):
        try:
            callback = self.scm + "://" + self.netloc + "callback.html"
            auth = tweepy.OAuthHandler(os.environ["consumer_key"], os.environ["consumer_secret"], callback=callback, secure=True)
            session = self.server.getSession(self)
            rt = session['request_token']
            auth.set_request_token(rt["key"], rt["secret"])
            verifier = self.params["oauth_verifier"][0]
            session["token"] = auth.get_access_token(verifier)
            self.__loadUser()
        except:
            logger.exception("Error")
        self.__sendRedirect("/")

    def GET_UserTypeAheadJson(self):
        if not "q" in self.params:
            logger.info("Missing param")
            self.send_error(400)
            return
        self.send_response(200, "OK")
        self.send_header("Content-Type", "application/json")
        api = self.__api()
        self.end_headers()
        users = api.search_users(q=self.params["q"][0])
        userNameList = []
        for user in users:
            userNameList.append(user.screen_name)
        result = {"options": userNameList}
        self.wfile.write(simplejson.dumps(result))

    def __loadUser(self):
        api = self.__api()
        user = api.verify_credentials()
        logger.info(unicode(user))
        session = self.server.getSession(self)
        session["user"] = user
        #self.server.runDataCrawler(session, user.id, user.screen_name)
        return user

    def __api(self):
        session = self.server.getSession(self)
        return _buildApiUsingSession(session)

    def GET_LogonHtml(self):
        auth = tweepy.OAuthHandler(os.environ["consumer_key"], os.environ["consumer_secret"], callback="http://localhost:8888/callback.html", secure=True)
        try:
            auth_url = auth.get_authorization_url()
            session = self.server.getSession(self)
            session['request_token'] = {"key": auth.request_token.key, "secret": auth.request_token.secret}
            self.__sendRedirect(auth_url)
        except TweepError as e:
            logger.exception(e)
            self.__sendRedirect("/")

    def GET_LogoutHtml(self):
        self.server.removeSession(self)
        self.__sendRedirect("/")

    def __sendFile(self, filename):
        try:
            b, ext = os.path.splitext(filename)
            f = open(os.path.join(os.getcwd(), filename), "r")
            self.send_response(200, "OK")
            self.send_header("Content-Type", ext2mime.get(ext, "application/octet-stream"))
            self.end_headers()
            self.wfile.write(f.read())
        except IOError as e:
            self.send_response(404)
            logger.info(u"Cannot send file: " + unicode(e))
        finally:
            if f:
                f.close()

    def __sendRedirect(self, url):
        logger.info("Redirect to " + url)
        self.send_response(302, "Moved Temporarily")
        self.send_header("Location", url)
        self.end_headers()

    def GET_static(self):
        dirRes = os.path.abspath(os.path.join(os.getcwd(), "./resources/"))
        filename = os.path.join(dirRes, self.path[1:])
        filename = os.path.abspath(filename)
        if filename.startswith(dirRes):
            self.__sendFile(filename)
        else:
            logger.warn(u"Cannot send file " + filename + u": must be in " + dirRes)
            self.send_response(404)

    def GET_indexHtml(self):
        self.__sendFile(os.path.join(os.getcwd(), "./resources/main.html"))

    def GET_(self):
        return self.GET_indexHtml()

    def do_HEAD(self):
        self.send_response(200, "OK")
        self.send_header(" Access-Control-Allow-Origin", "*")
        self.end_headers()

    def send_header_lazy(self, name, value):
        self.__lazyHeaders.append((name, value))

    def end_headers(self):
        for name, value in self.__lazyHeaders:
            self.send_header(name, value)
        BaseHTTPRequestHandler.end_headers(self);


class UserDataCrawler(StoppableThread):

    def __init__(self, httpSession, userMgr, userId=None, screenName=None):
        StoppableThread.__init__(self)
        self.__session = httpSession
        self.__userMgr = userMgr
        self.__userId = userId
        self.__screenName = screenName
        self.__token = self.__session["token"]

    def runPart(self):
        userFeatures = self.__userMgr.doJob(self.__token, self.__userId, self, self.__screenName)
        sessionKey = "features-" + userFeatures.screenName()
        self.__session[sessionKey] = userFeatures
        logger.info("Store user features in session[\"" + sessionKey +"\"")
        try:
            userFeatures.doJob()
        except NothingToDo as e:
            raise e
        except BaseException as e:
            logger.exception(u"UserFeatures.doJob error for " + unicode(self.__userId) + u" " + unicode(self.__screenName))
            raise e
        raise NothingToDo()

class EmbeddedHttpServer(StoppableThread, SocketServer.TCPServer):

    def __init__(self, model, userMgr):
        StoppableThread.__init__(self)
        self.__sessions = {}
        self.__userMgr = userMgr
        self.__workers = []
        self.__sessionLock = Semaphore()
        self.__PORT = 8888
        SocketServer.TCPServer.__init__(self, ("", self.__PORT), PynewsHandler)
        self.__model = model
        dir = os.path.expanduser("~/.pynews")
        if not os.path.exists(dir):
            os.mkdir(dir)
        file = os.path.join(dir, "settings.web.db")
        self.__settingsStore = shelve.open(file, protocol=-1)

    def urls(self, cat=None):
        return self.__model.finalUrls(cat)

    def getHref(self):
        return "http://localhost:" + str(self.__PORT)

    def createSessionCookie(self, handler):
        logger.info(u"Create new session cookie")
        expiration = datetime.datetime.now() + datetime.timedelta(days=30)
        cookie = Cookie.SimpleCookie()
        cookie["session"] = base64.encodestring(str(random.randint(0, 1000000000)))
        #cookie["session"]["domain"] = handler.
        cookie["session"]["path"] = "/"
        cookie["session"]["expires"] =\
        expiration.strftime("%a, %d-%b-%Y %H:%M:%S PST")
        return cookie

    def readSessionId(self, headers):
        if "Cookie" in headers:
            c = Cookie.SimpleCookie(headers["Cookie"])
            if "session" in c:
                return c['session'].value
        return None

    def getToken(self):
        return None#TODO

    def findSession(self):
        try:
            cookie = Cookie.SimpleCookie(os.environ["HTTP_COOKIE"])
            print "session = " + cookie["session"].value
        except (Cookie.CookieError, KeyError):
            print "session cookie not set!"

    def getSession(self, handler):
        try:
            self.__sessionLock.acquire()
            headers = handler.headers
            sessionId = self.readSessionId(headers)
            if sessionId is None or sessionId not in self.__sessions.keys():
                newCookie = self.createSessionCookie(handler)
                sessionId = newCookie["session"].value
                logger.info(u"Create session " + unicode(sessionId))
                self.__sessions[sessionId] = {}
                handler.send_header_lazy('Set-Cookie', newCookie.output(header=''))
            return self.__sessions[sessionId]
        finally:
            self.__sessionLock.release()

    def removeSession(self, handler):
        headers = handler.headers
        sessionId = self.readSessionId(headers)
        if sessionId in self.__sessions.keys():
            logger.info(u"Remove session " + unicode(sessionId))
            del self.__sessions[sessionId]
        else:
            logger.info(u"Cannot remove session " + unicode(sessionId))

    def runDataCrawler(self, session, userId=None, screenName=None):
        c = UserDataCrawler(session, self.__userMgr, userId, screenName)
        self.__workers.append(c)
        c.start()

    def saveSettings(self, who, settings):
        logger.info(u"Save settings for " + unicode(who) + u": " + unicode(settings))
        self.__settingsStore[who] = settings
        self.__settingsStore.sync()

    def readSettings(self, who, default=None):
        return self.__settingsStore.get(who, default)

    def atBegin(self):
        logger.info("serving at port" + str(self.__PORT))

    def atEnd(self):
        StoppableThread.atEnd(self)
        self.__settingsStore.close()

    def runPart(self):
        self.serve_forever()
        raise NothingToDo()

    def stop(self):
        self.shutdown()
        for w in self.__workers:
            w.stop()

if __name__ == "__main__":
    server = EmbeddedHttpServer([])
    def signal_handler(signal, frame):
        server.stop()
        logger.info('You pressed Ctrl+C!')
    signal.signal(signal.SIGINT, signal_handler)
    server.start()
