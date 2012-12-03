# -*- coding: utf-8 *-*
from BaseHTTPServer import BaseHTTPRequestHandler
import SocketServer
import os
import urllib2
import urlparse
import signal
import simplejson
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

class NewsMapNode:

    def __init__(self):
        self.__children = list()
        self.__id = None
        self.__title = None
        self.__html = None
        self.__text = None
        self.__mark = 0
        self.__tweets = None
        self.__href = None

    def setTweets(self, tweets):
        self.__tweets = tweets

    def getTweets(self):
        return self.__tweets

    def setHtml(self, html):
        self.__html = html

    def getHtml(self):
        return self.__html

    def setText(self, text):
        self.__text = text

    def getText(self):
        return self.__text

    def setMark(self, a):
        self.__mark = a

    def getMark(self):
        return self.__mark

    def getHref(self):
        return self.__href

    def setHref(self, href):
        self.__href = href

    def getChildren(self):
        return self.__children

    def setChildren(self, children):
        self.__children = children

    def getId(self):
        return self.__id

    def setId(self, id):
        self.__id = id

    def getTitle(self):
        return self.__title

    def setTitle(self, title):
        self.__title = title

    def dump(self):
        return {
            u"id": self.getId(),
            u"title": self.getTitle(),
            u"mark": self.getMark(),
            u"href": self.getHref(),
            #u"html": self.getHtml(),
            u"text": self.getText(),
            u"tweets": [t.dump() for t in self.getTweets()],
            u"children": map(NewsMapNode.dump, self.getChildren()),
        }

class PynewsHandler(BaseHTTPRequestHandler):

    def __buildData(self, label):
        cat = label2code.get(label)
        allUrls = self.server.urls()
        selectedUrls = allUrls if cat is None else [url for url in allUrls if cat in url.documentClasses()]

        roots = {}
        id = 0
        for url in selectedUrls:
            child = NewsMapNode();
            child.setId(unicode(id));
            child.setMark(url.mark())
            child.setTitle(url.getTitle());
            child.setText(url.getText())
            child.setHtml(url.getHtml())
            child.setHref(url.getExpandedUrl())
            child.setTweets(url.tweets())
            urlCat = self.__findLabel(url)
            k = code2label[urlCat]
            if not roots.has_key(k):
                roots[k] = self.__buildRootNode(k)
            parent = roots[k]
            parent.getChildren().append(child);
            id += 1
        if len(roots.values()) > 1:
            root = NewsMapNode()
            root.setId(u"root")
            root.setTitle(unicode(label))
            root.setChildren(roots.values())
            root.setTweets([])
        else:
            root = roots[label] if roots.has_key(label) else self.__buildRootNode(label)
        return root.dump()

    def __findLabel(self, url):
        cats = url.documentClasses()
        for c in cats:
            if c not in ["short", "medium", "long"]:
                return c
        raise ValueError(unicode(cats))

    def __buildRootNode(self, label):
        root = NewsMapNode()
        root.setId(u"root-" + unicode(label))
        root.setTitle(unicode(label))
        root.setTweets([])
        return root

    def do_GET(self):
        (self.scm, self.netloc, self.path, self.params, self.query, self.fragment) = urlparse.urlparse(self.path, 'http')
        self.params = urlparse.parse_qs(self.query)
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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        d = self.__buildData(self.params["c"][0])
        self.wfile.write(simplejson.dumps({"data": d}))

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

class EmbeddedHttpServer(StoppableThread, SocketServer.TCPServer):

    def __init__(self, urlsList):
        StoppableThread.__init__(self)
        self.__PORT = 8888
        SocketServer.TCPServer.__init__(self, ("", self.__PORT), PynewsHandler)
        self.__urls = urlsList

    def urls(self):
        return self.__urls

    def atBegin(self):
        logger.info("serving at port" + str(self.__PORT))

    def runPart(self):
        self.serve_forever()
        raise NothingToDo()

    def stop(self):
        self.shutdown()


if __name__ == "__main__":
    server = EmbeddedHttpServer([])
    def signal_handler(signal, frame):
        server.stop()
        logger.info('You pressed Ctrl+C!')
    signal.signal(signal.SIGINT, signal_handler)
    server.start()
