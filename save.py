# -*- coding: utf-8 *-*
from fileinput import filename

__author__ = 'soldier'

import sys
import tweetstream
import struct
import gzip
import pickle
import os
from os import path
from datetime import datetime
from datetime import timedelta

class TweetIterator(object):

    def __init__(self, file):
        self.__file = file

    def __iter__(self):
        return self

    def next(self):
        buf = self.__file.read(4)
        if not buf:
            raise StopIteration()
        size = struct.unpack('i', buf)[0]
        data = self.__file.read(size)
        tweet = pickle.loads(data)
        return tweet

class Manager:

    def store(self, dir):

        filename = self.filenameWrite(dir)
        f_out = gzip.open(filename, 'wb')
        start = datetime.now()
        dayDelta = timedelta(hours=5)
        i = 0
        c = 0
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
                        print u"Close file " + filename + u" (" + unicode(c) + u")"
                        f_out.close()
                        filename = self.filenameWrite(dir)
                        f_out = gzip.open(filename, 'wb')
                        start = datetime.now()
                else:
                    i += 1
                c += 1
        except KeyboardInterrupt:
            print u"Close file " + filename + u" (" + unicode(c) + u")"
            f_out.close()

    def restore(self, filename):
        f = self.filenameRead(filename)
        print u"Open: " + f
        f_in = gzip.open(f, "rb")
        return TweetIterator(f_in)


    def filenameWrite(self, dir):
        if not path.exists(dir):
            os.makedirs(dir)
        now = datetime.now()
        return path.join(dir, unicode(now) + u".tweets")

    def filenameRead(self, filename):
        if path.isfile(filename):
            return filename

        newest = None
        time = None
        for dirname, dirnames, filenames in os.walk(filename):
            for filename in filenames:
                if filename.endswith(u".tweets"):
                    file = path.join(dirname, filename)
                    if time == None or time < os.path.getmtime(file):
                        time = os.path.getmtime(file)
                        newest = file
        return newest

if __name__ == "__main__":
    m = Manager()
    dir = u"/tmp/tweets/" if len(sys.argv) < 3 else sys.argv[2]
    method = u"s" if len(sys.argv) < 2 else sys.argv[1]
    if method == u"s":
        m.store(dir)
    elif method == u"r":
        for t in m.restore(dir):
            print t
