# -*- coding: utf-8 *-*
__author__ = 'soldier'

import sys
import tweetstream
import struct
import gzip
import pickle
import os
import re
from os import path
from datetime import datetime
from datetime import timedelta
from logger import logger

filenamePattern = re.compile(r"s=([^,]+),c=([^,]+).tweets")

class TweetIterator(object):

    def __init__(self, files):
        self.__fileIdx = 0
        self.__tweetIdx = 0
        self.__tweetInFileCounter = 0
        self.__file = files[self.__fileIdx]
        self.__all = files
        self.__count = 0
        for file in files:
            val = self.__tweetsInFile(file)
            if val:
                self.__count += val

    def __isLastFile(self):
        return self.__fileIdx == len(self.__all) - 1

    def __getNextFile(self):
        gzFile = self.__all[self.__fileIdx]
        gzFile.close()
        if not self.__getFilenameMatcher(gzFile):
            self.__rename(gzFile, self.__tweetInFileCounter)
        self.__fileIdx += 1
        self.__tweetInFileCounter = 0
        return self.__all[self.__fileIdx]

    def __rename(self, gzFile, count):
        filename = gzFile.filename
        newFilename = os.path.join(os.path.dirname(filename), "s=" + os.path.basename(filename).replace(".tweets", "") + ",c=" + str(count) + ".tweets")
        logger.info("Rename " + filename + " to " + newFilename)
        os.rename(filename, newFilename)

    def __iter__(self):
        return self

    def position(self):
        return self.__tweetIdx

    def count(self):
        return self.__count

    def currentFile(self):
        return self.__fileIdx + 1

    def filesCount(self):
        return len(self.__all)

    def __tweetsInFile(self, file):
        m = self.__getFilenameMatcher(file)
        if m:
            return m.group(2)
        else:
            return None

    def __getFilenameMatcher(self, file):
        return filenamePattern.match(file.name.replace(".gz", ""))

    def next(self):
        buf = self.__file.read(4)
        if not buf :
            if self.__isLastFile():
                raise StopIteration()
            else:
                self.__file = self.__getNextFile()
                #print "Open next " + str(self.__file)
                buf = self.__file.read(4)
        size = struct.unpack('i', buf)[0]
        data = self.__file.read(size)
        tweet = pickle.loads(data)
        self.__tweetIdx += 1
        self.__tweetInFileCounter += 1
        return tweet

class Manager:

    def store(self, dir):

        filename = self.filenameWrite(dir)
        f_out = gzip.open(filename, 'wb')
        start = datetime.now()
        dayDelta = timedelta(hours=5)
        i = 0
        tweetInStreamCounter = 0
        tweetInFileCounter = 0
        try:
            stream = tweetstream.SampleStream(u'soldierkam', os.environ["PASSWORD"])
            for s in stream:
                data = pickle.dumps(s)
                f_out.write(struct.pack('i', len(data)))
                f_out.write(data)
                if i > 100:
                    i = 0
                    print "."
                    delta = datetime.now() - start
                    if delta > dayDelta:
                        print u"Close file " + filename + u" (" + unicode(tweetInStreamCounter) + u")"
                        f_out.close()
                        self.__renameFile(filename, tweetInFileCounter)
                        filename = self.filenameWrite(dir)
                        f_out = gzip.open(filename, 'wb')
                        start = datetime.now()
                        tweetInFileCounter = 0
                else:
                    i += 1
                tweetInStreamCounter += 1
                tweetInFileCounter += 1
        except KeyboardInterrupt:
            print u"Close file " + filename + u" (" + unicode(tweetInStreamCounter) + u")"
            f_out.close()

    def __renameFile(self, filename, start, tweetsCount):
        dir = os.path.dirname(filename)
        newFilename = os.path.join(dir, "s=" + str(start) + ",c=" + tweetsCount + ",e=" + str(datetime.now()) + ".tweets")
        os.rename(filename, newFilename)

    def restore(self, filename, lastOnly=False):
        filesNames = self.filenameRead(filename)
        print u"Open: " + unicode(map(str, filesNames))
        f_in=[]
        filesNames = [filesNames[-1]] if lastOnly else filesNames
        for f in filesNames:
            f_in.append(gzip.open(f, "rb"))
        return TweetIterator(f_in)

    def filenameWrite(self, dir):
        if not path.exists(dir):
            os.makedirs(dir)
        now = datetime.now()
        return path.join(dir, unicode(now) + u".tweets")

    def filenameRead(self, filename):
        if path.isfile(filename):
            return [filename]

        result = []
        for dirname, dirnames, filenames in os.walk(filename):
            for filename in filenames:
                if filename.endswith(u".tweets"):
                    file = path.join(dirname, filename)
                    result.append(file)
        if not result:
            raise Exception("No file in " + filename)
        return sorted(result, key=os.path.getmtime)

if __name__ == "__main__":
    m = Manager()
    dir = u"/tmp/tweets/" if len(sys.argv) < 3 else sys.argv[2]
    method = u"s" if len(sys.argv) < 2 else sys.argv[1]
    if method == u"s":
        m.store(dir)
    elif method == u"r":
        for t in m.restore(dir):
            print t
