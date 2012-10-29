# -*- coding: utf-8 -*-
import gzip
import sys,re,urllib2,base64,mimetypes,urlparse
import threading
from urllib2 import HTTPError, HTTPRedirectHandler
from BeautifulSoup import BeautifulSoup, Tag, CData, NavigableString
import chardet
from os import path
from magic import Magic

INLINE = "inline"
DO_NTH = "do_nothing"
REMOVE = "remove"

ACTIONS = [INLINE, DO_NTH, REMOVE]

class ContentResolver():

    def __init__(self, logger):
        self.__content_cache = {}
        self.__logger = logger

    def getContent(self, url, expect_binary = None):
        if not isinstance(url, MediaUrl):
            url = MediaUrl(url)
        key = url.getId()
        if self.__content_cache.has_key(key):
            val = self.__content_cache[key]
            url.replaceWith(val["url"])
            return val["content"], val["mime"]

        threadName = threading.currentThread().name
        self.__logger.debug(threadName + u": download " + unicode(url))
        if url.isBase64():
            return url.getCurrentUrl(), None

        if url.isRemote():
            content = url.downloadContent()
            if not self.isBinary(expect_binary, url, content):
                encoding = self.getEncoding(content)
                self.__logger.debug("Apply charset " + str(encoding) + " to " + str(url))
                s = unicode(content.data(), encoding)
                self.__content_cache[key] = {"content":s, "url": url, "mime": content.mime()}
                return s, content.mime()
            else:
                self.__content_cache[key] = {"content":content.data(), "url": url, "mime": content.mime()}
                return content.data(), content.mime()
        else:
            s = open(url.getCurrentUrl()).read()
            if not expect_binary:
                encoding = chardet.detect(s)
                s = s.decode(encoding['encoding'])
            self.__content_cache[key] = s
            return s, None

    def getEncoding(self, content):
        if content.encoding():
            return content.encoding()
        idx = content.data().find("@charset")
        if idx != -1:
            m = re.search(r"^@charset[\s]+['\"]{0,1}([a-zA-Z0-9\-]+)['\"]{0,1}", content.data())
            if m:
                return m.group(1)
        enc = chardet.detect(content.data())['encoding']
        if not enc:
            enc = "iso-8859-1"
        return enc

    def isBinary(self, expectBinary, url, content):
        if expectBinary != None:
            return expectBinary
        mime = content.mime()
        if mime and mime.startswith("text/"):
            return False
            #if url.getExtension() in ["css", "html", "js", "htm"]:
        #    return False
        return True

    def getEncodedContent(self, url, expect_binary=None):
        content, mime = self.getContent(url, expect_binary)
        mime = mime or mimetypes.guess_type(url.getCurrentUrl())[0]
        if not mime:
            raise ValueError(u"Unknown mime " + unicode(url))
        return u'data:%s;base64,%s' % (mime, base64.standard_b64encode(content.encode("UTF-8") if type(content) == unicode else content))


class MyNavigableString(NavigableString):

    def __str__(self, encoding="utf-8"):
        if encoding:
            return self.encode(encoding)
        else:
            return self

class Content():

    magicMime = Magic(mime_encoding=True)

    def __init__(self, data, contentType):
        self.__d = data
        if contentType:
            if contentType.find("charset") != -1:
                m = re.compile(r"([\S]+)[ ]*;[ ]*charset[ :=]+(\S+)").search(contentType)
                self.__e = m.group(2)
                self.__m = m.group(1)
            else:
                self.__e = None
                self.__m = contentType
        else:
            self.__e = None
            self.__m = None

        if not self.__m:
            self.__m = Content.magicMime.from_buffer(data)

    def mime(self):
        return self.__m

    def encoding(self):
        return self.__e

    def data(self):
        return self.__d

class MediaUrl():

    def __init__(self, url, logger):
        self.__logger = logger
        if isinstance(url, MediaUrl):
            url = url.__current
        self.__url = url
        self.__current = url
        self.__enc = None

    def __str__(self):
        return self.__current

    def isRemote(self):
        return urlparse.urlparse(self.__current)[0] in ('http','https', '')

    def downloadContent(self):
        url_blacklist = ('getsatisfaction.com',
                         'google-analytics.com',)
        for bli in url_blacklist:
            if self.__url.find(bli) != -1:
                return None

        if self.__current.startswith("data:"):
            return Content(self.__current, None)

        if isinstance(self.__current, unicode):
            url = self.__current.encode("UTF-8")
        else:
            url = self.__current
        opener = urllib2.build_opener(HTTPRedirectHandler())
        #opener.addheaders = [('User-agent', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4')]
        ct = opener.open(url)
        self.__current = ct.geturl()
        contentType = ct.headers["content-type"] if ct.headers.has_key("content-type") else None
        return Content(ct.read(), contentType)

    def setEncoding(self, enc):
        self.__enc = enc

    def getEncoding(self):
        return self.__enc

    def getUrl(self):
        return self.__current

    def resolve(self, path):
        if path.startswith("data:"):
            return MediaUrl(path, self.__logger)
        return MediaUrl(urlparse.urljoin(self.__current, path), self.__logger)

    def isBase64(self):
        return self.__current.startswith("data:")

    def getId(self):
        return self.__url

    def replaceWith(self, otherUrl):
        self.__logger = otherUrl.__logger
        self.__url = otherUrl.__url
        self.__current = otherUrl.__current

    def getCurrentUrl(self):
        return self.__current

    def getExtension(self):
        return path.splitext(self.getCurrentUrl())[1][1:].split("?")[0].lower()

class Replacer:

    def __init__(self, cssUrl, contentResolver, logger):
        self.__base = cssUrl.resolve('.')
        self.__parent = cssUrl
        self.__contentResolver = contentResolver
        self.__logger = logger

    def getEncodedPath(self, pathName):
        url = None
        try:
            url = self.__base.resolve(pathName)
            if url.isBase64():
                return url.getCurrentUrl()
            return self.__contentResolver.getEncodedContent(url)
        except HTTPError as e:
            self.__logger.error("Cannot replace url=" + str(url) + " with content (parent url=" + str(self.__parent) + ", base url= " + str(self.__base) + ", path=" + str(pathName) + "): " + str(e))
        except BaseException as e:
            self.__logger.error("Cannot replace url=" + str(url) + " with content (parent url=" + str(self.__parent) + ", base url= " + str(self.__base) + ", path=" + str(pathName) + ")")
            self.__logger.exception(e)
        return u"about:blank"

    def __call__(self, *args, **kwargs):
        return u"url(" + self.getEncodedPath(args[0].group(1).strip().replace("\"", "").replace("'", "")) + u")"

class ImportReplacer:

    def __init__(self, cssUrl, contentResolver, logger):
        self.__base = cssUrl.resolve('.')
        self.__parent = cssUrl
        self.__contentResolver = contentResolver
        self.__logger = logger

    def getEncodedPath(self, pathName):
        url = None
        try:
            url = self.__base.resolve(pathName)
            return self.__contentResolver.getContent(url)[0]
        except HTTPError as e:
            self.__logger.error("Cannot replace import url=" + str(url) + " with content (parent url=" + str(self.__parent) + ", base url= " + str(self.__base) + ", path=" + str(pathName) + "): " + str(e))
        except BaseException as e:
            self.__logger.error("Cannot replace import url=" + str(url) + " with content (parent url=" + str(self.__parent) + ", base url= " + str(self.__base) + ", path=" + str(pathName) + ")")
            self.__logger.exception(e)
        return u""

    def __call__(self, *args, **kwargs):
        return self.getEncodedPath(args[0].group(1).strip().replace("\"", "").replace("'", ""))

class Downloader():

    def __init__(self, logger=None, js=REMOVE, img=INLINE, css=INLINE, iframes=INLINE, compress=False):
        if not logger:
            import logging
            from logging import StreamHandler
            logger = logging.getLogger()
            logger.setLevel(logging.DEBUG)
            handler = StreamHandler()
            handler.setLevel(logging.DEBUG)
            logger.addHandler(handler)
        self.__logger = logger
        self.__compress = compress
        self.__contentResolver = ContentResolver(self.__logger)
        self.__js = self.__validateAction(js)
        self.__img = self.__validateAction(img)
        self.__css = self.__validateAction(css)
        self.__iframes = self.__validateAction(iframes)

    def copy(downloader):
        return Downloader(downloader.__logger, downloader.__js, downloader.__img, downloader.__css, downloader.__iframes, downloader.__compress)

    def __validateAction(self, action):
        if action not in ACTIONS:
            raise ValueError("Unknown action " + action)
        return action

    def download(self, url, filename):
        return self.__downloadWebpage(MediaUrl(url, self.__logger), filename)

    def __downloadWebpage(self, mediaUrl, outputFilename=None):
        content = self.__contentResolver.getContent(mediaUrl, False)[0]

        bs = BeautifulSoup(content)
        self.__replaceRelativeUrls(mediaUrl,bs)
        self.__replaceExternalResourceInStyles(mediaUrl,bs)
        self.__replaceJavascript(mediaUrl,bs)
        self.__replaceCss(mediaUrl,bs)
        self.__replaceImages(mediaUrl,bs)
        self.__replaceIframes(mediaUrl,bs, outputFilename)
        if outputFilename:
            self.__writeToFile(bs, outputFilename)
        else:
            return bs.renderContents()

    def __writeToFile(self, bs, filename):
        if self.__compress:
            f = gzip.open(filename, 'wb')
        else:
            f = open(filename,'wb')
        metaTag = bs.find("meta", {"charset": re.compile('.+')})
        if not metaTag:
            head = bs.find("head")
            if not head:
                html = bs.find("html")
                if html:
                    head = Tag(bs, "head")
                    html.insert(0, head)
                else:
                    head = None
            if head:
                metaTag = Tag(bs, "meta")
                head.insert(0, metaTag)
        if metaTag:
            metaTag["charset"] = "UTF-8"
        f.write(bs.renderContents())
        f.close()

    def __replaceCss(self, baseUrl, soup):
        if self.__css != DO_NTH:
            for css in soup.findAll('link',{'rel':'stylesheet','href':re.compile('.+')}):
                try:
                    cssHref = css['href']
                    cssUrl = baseUrl.resolve(cssHref)
                    if self.__css == INLINE:
                        data = self.__contentResolver.getContent(cssUrl, False)[0]
                        cssContent = self.__inlineExternalResourcesInCss(cssUrl, data)
                    else:
                        cssContent = u"<!--" + str(cssUrl) + u"-->"
                    newStyleTag = Tag(soup, "style")
                    newStyleTag.insert(0,  MyNavigableString(cssContent))
                    if css.get('media'):
                        newStyleTag['media'] = css['media']
                    css.replaceWith(newStyleTag)
                except BaseException as e:
                    self.__logger.exception(u'failed to load css from %s' % css['href'])
                    #css.replaceWith('<!-- failed to load css from %s -->' % css['href'])

    def __replaceImages(self, base_url,soup):
        from itertools import chain
        srcAttrName = ["data-lazy-src", "data-src", "src"]
        for img in chain(soup.findAll('img',{'src':re.compile('.+')}),
            soup.findAll('input',{'type':'image','src':re.compile('.+')})):
            try:
                for attrName in srcAttrName:
                    if img.get(attrName):
                        src = img[attrName]
                        break
                path = base_url.resolve(src)
                img['src'] = path.getCurrentUrl() if path.isBase64() else self.__contentResolver.getEncodedContent(path)
            except BaseException as e:
                self.__logger.exception(u'failed to load image from %s' % img['src'])

    def __replaceExternalResourceInStyles(self, baseUrl,soup):
        for t in soup.findAll(attrs={'style':re.compile(r'url\([^)]+\)')}):
            try:
                style = t["style"]
                style = self.__inlineExternalResourcesInCss(baseUrl, style)
                t['style'] = style
            except Exception as e:
                self.__logger.exception(u'failed to load image into %s' % t['style'])

    def __replaceRelativeUrls(self, baseUrl,soup):
        for t in soup.findAll(attrs={'href':re.compile('.+')}):
            href = t["href"]
            try:
                if href.startswith("/") and not href.startswith("//"):
                    href = self.__resolvePath(baseUrl, href)
                t["href"] = href
            except BaseException as e:
                self.__logger.exception(u'failed to load image into %s' % href)

    def __replaceJavascript(self, baseUrl,soup):
        if self.__js != DO_NTH:
            for js in soup.findAll('script'):
                src = js.get("src") if js.get('src') else None
                try:
                    if src and self.__js == INLINE:
                        jsContent = self.__contentResolver.getContent(baseUrl.resolve(src), False)
                    elif self.__js == REMOVE:
                        u = str(baseUrl.resolve(src)) if src else "inlined"
                        jsContent = "<!--" + u + "-->"
                    else:
                        #nothing to change
                        continue
                    newScriptTag = Tag(soup, "script")
                    newScriptTag.insert(0,  MyNavigableString(jsContent))
                    js.replaceWith(newScriptTag)
                except BaseException as e:
                    self.__logger.error(u'failed to load javascript from %s' % unicode(src))

    def __replaceIframes(self, baseUrl, soup, outputFilename):
        baseFilename, extension = path.splitext(outputFilename)
        c = 0
        if self.__iframes != DO_NTH:
            for iframe in soup.findAll('iframe'):
                src = iframe.get("src") if iframe.get('src') else None
                try:
                    if src and self.__iframes == INLINE:
                        u = baseFilename + "_" + str(c) + extension
                        self.download(baseUrl.resolve(src), u)
                        iframe["src"] = path.basename(u)
                        iframe.insert(0,  MyNavigableString(u"<!-- " + unicode(src) + u" -->"))
                    elif self.__js == REMOVE:
                        u = str(baseUrl.resolve(src)) if src else "about:blank"
                        iframe["src"] = u
                        iframe.insert(0,  MyNavigableString(u"<!-- " + unicode(src) + u" -->"))
                    else:
                        #nothing to change
                        continue

                except BaseException as e:
                    self.__logger.exception(u'failed to load iframe from %s' % unicode(src) + " (base url = " + unicode(baseUrl) + ")")
                c += 1

    def __inlineExternalResourcesInCss(self, baseUrl, cssContent):
        cssContent = re.compile(ur'@import[ ]+url\(([^\)]+)\)').sub(ImportReplacer(baseUrl, self.__contentResolver, self.__logger), cssContent)
        return re.sub(ur'url\(([^\)]+)\)', Replacer(baseUrl, self.__contentResolver, self.__logger), cssContent)


    def __resolvePath(self, base, target):
        return urlparse.urljoin(base.getCurrentUrl(),target)

    def __getContent(self, url, binary=True):
        return self.__contentResolver.getContent(url, binary)

if __name__ == '__main__':
    d = Downloader()
    d.download(sys.argv[1], sys.argv[2])

