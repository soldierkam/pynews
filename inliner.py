# -*- coding: utf-8 -*-

import sys,re,urllib2,base64,mimetypes,urlparse
import threading
from urllib2 import HTTPError
from BeautifulSoup import BeautifulSoup, Tag, CData, NavigableString
import chardet
from logger import logger


def is_remote(address):
    return urlparse.urlparse(address)[0] in ('http','https', '')

def ignore_url(address):
    url_blacklist = ('getsatisfaction.com',
                     'google-analytics.com',)

    for bli in url_blacklist:
        if address.find(bli) != -1:
            return True

    return False

content_cache = {}

def get_content(from_,expect_binary=False):
    key = from_
    if content_cache.has_key(key):
        val = content_cache[key]
        return val["content"], val["url"]

    if is_remote(from_):
        if from_.startswith("//"):
            from_ = "http:" + from_

        if ignore_url(from_):
            return u'', from_

        threadName = threading.currentThread().name
        logger.debug(threadName + ": download " + from_)
        ct = urllib2.urlopen(from_)
        final_url = ct.geturl()
        if not expect_binary:
            rawdata = ct.read()
            encoding = chardet.detect(rawdata)
            s = rawdata.decode(encoding['encoding'])
            #encodings = feedparser._getCharacterEncoding(ct.headers,s)
            content_cache[key] = {"content":s, "url": final_url}
            return s, final_url
        else:
            s = ct.read()
            content_cache[key] = {"content":s, "url": final_url}
            return s, final_url
    else:
        s = open(from_).read()
        if not expect_binary:
            encoding = chardet.detect(s)
            s = s.decode(encoding['encoding'])
        content_cache[key] = s
        return s, from_

def get_encoded_content(url):
    content, finalurl = get_content(url, True)
    mime = mimetypes.guess_type(url)[0]
    return u'data:%s;base64,%s' % (mime, base64.standard_b64encode(content))

def resolve_path(base,target):
    return urlparse.urljoin(base,target)

def replaceJavascript(base_url,soup):
    for js in soup.findAll('script'):
        try:
            #real_js = get_content(resolve_path(base_url,js['src']))
            js.replaceWith("")
        except Exception,e:
            print 'failed to load javascript from %s' % js['src']
            print e

#css_url1 = re.compile(ur'url\("([^"]+)"\)')
#css_url2 = re.compile(ur'url\(\'([^\']+)\'\)')
css_url3 = re.compile(ur'url\(([^\)]+)\)')
css_patterns = [css_url3]

class Replacer:

    def __init__(self, cssUrl):
        cssUrl = unicode(cssUrl)
        self.__base = resolve_path(cssUrl, '.')
        self.__parent = cssUrl

    def __replac(self, p):
        path = str(p.groups()[0]).replace("'", "").replace("\"", "")
        url = None
        try:
            url = resolve_path(self.__base, path)
            return "url(%s)" % get_encoded_content(url)
        except HTTPError as e:
            logger.error("Cannot replace url=" + str(url) + " with content (parent url=" + self.__parent + ", base url= " + self.__base + ", path=" + path + "): " + str(e))
        except BaseException as e:
            logger.error("Cannot replace url=" + str(url) + " with content (parent url=" + self.__parent + ", base url= " + self.__base + ", path=" + path + ")")
            logger.exception(e)
        return "url(about:blank)"

    def __call__(self, *args, **kwargs):
        return self.__replac(args[0])

def __inlineImgInCss(cssUrl, cssContent):
    for p in css_patterns:
        cssContent = re.sub(p,Replacer(cssUrl),cssContent)
    return cssContent

class MyNavigableString(NavigableString):

    def __str__(self, encoding="utf-8"):
        # Substitute outgoing XML entities.
        data = self
        if encoding:
            return data.encode(encoding)
        else:
            return data

def replaceCss(base_url,soup):
    for css in soup.findAll('link',{'rel':'stylesheet','href':re.compile('.+')}):
        try:
            cssHref = css['href']
            cssUrl = resolve_path(base_url,cssHref)
            real_css,finalUrl = get_content(cssUrl)

            newStyleTag = Tag(soup, "style")
            newStyleTag.insert(0,  MyNavigableString(__inlineImgInCss(finalUrl, real_css)))
            if css['media']:
                newStyleTag['media'] = css['media']
            css.replaceWith(newStyleTag)

        except Exception,e:
            logger.exception('failed to load css from %s' % css['href'])
            #css.replaceWith('<!-- failed to load css from %s -->' % css['href'])

def replaceImages(base_url,soup):
    from itertools import chain

    for img in chain(soup.findAll('img',{'src':re.compile('.+')}),
                     soup.findAll('input',{'type':'image','src':re.compile('.+')})):
        try:
            path = resolve_path(base_url,img['src'])
            img['src'] = get_encoded_content(path)
        except Exception,e:
            logger.exception('failed to load image from %s' % img['src'])

def replaceImagesInStyles(base_url,soup):
    for t in soup.findAll(attrs={'style':re.compile(r'url\([^)]+\)')}):
        try:
            style = t["style"]
            style = __inlineImgInCss(base_url, style)
            t['style'] = style
        except Exception,e:
            logger.exception('failed to load image into %s' % t['style'])


def replaceRelativeUrls(base_url,soup):
    for t in soup.findAll(attrs={'href':re.compile('.+')}):
        try:
            href = t["href"]
            if href.startswith("/") and not href.startswith("//"):
                href = resolve_path(base_url, href)
            t["href"] = href
        except Exception,e:
            logger.exception('failed to load image into %s' % t['style'])

def downloadWebpage(url,output_filename):
    content,finalUrl = get_content(url)
    bs = BeautifulSoup(content)

    replaceRelativeUrls(finalUrl,bs)
    replaceImagesInStyles(finalUrl,bs)
    replaceJavascript(finalUrl,bs)
    replaceCss(finalUrl,bs)
    replaceImages(finalUrl,bs)
    res = open(output_filename,'wb')
    print >>res,str(bs)
    res.close()

if __name__ == '__main__':
    downloadWebpage(sys.argv[1],sys.argv[2])

