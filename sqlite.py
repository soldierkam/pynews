import os
import cPickle
from twisted.test.test_sslverify import counter
from logger import logger

__author__ = 'soldier'

import sqlite3

def _read(p):
    return cPickle.loads(p)

def _readOne(c):
    value = c.fetchone()
    if value is None:
        raise StopIteration()
    if len(value) != 2:
        raise ValueError(unicode(value))
    return value[0], _read(str(value[1]))

class Iterator():


    def __init__(self, c):
        c.execute('''SELECT ckey, cvalue FROM simple ''')
        self.__c = c

    def __iter__(self):
        return self

    def next(self):
        return _readOne(self.__c)

class SqliteShelve():

    def __init__(self, filename, flags="c", maxCacheSize=200, minCacheSize=100):
        self.__readonly = "r" in flags
        self.__maxCacheSize = maxCacheSize
        self.__minCacheSize = minCacheSize
        tmp, ext = os.path.splitext(filename)
        self.__sqliteFilename = filename
        if ext == ".db":
            sqliteFilename = tmp + ".db2"
        self.__conn = sqlite3.connect(self.__sqliteFilename)
        self.__conn.isolation_level = None
        self.__createTable()
        if self.__sqliteFilename != filename:
            self.__import(filename)
        self.__cache = {}
        self.__batch = []

    def __getitem__(self, item):
        item = self.__validateKey(item)
        if self.__cache.has_key(item):
            return self.__getFromCache(item)
        v = self.__getFromDb(item)
        self.__putInCache(item, v)
        return v

    def __setitem__(self, key, value):
        self.__validateWrite()
        key = self.__validateKey(key)
        self.__putInCache(key, value)
        self.__setToDb(key, value)

    def __delitem__(self, key):
        self.sync()
        if self.__cache.has_key(key):
            self.__delFromCache(key)
        c = self.__conn.cursor()
        c.execute('''DELETE FROM simple where ckey = ?''', (key,))

    def __validateWrite(self):
        if self.__readonly:
            raise ValueError("Read-only")

    def __import(self, shelveFilename):
        if not os.path.exists(shelveFilename):
            logger.info("Shelve not exists: " + str(shelveFilename))
            return
        c = self.__conn.cursor()
        logger.info("Open shelve")
        import shelve
        source = shelve.open(shelveFilename, "r")
        l = len(source)
        logger.info("Import data: " + str(l))
        i = 0
        p = 0
        for k, v in source.iteritems():
            i += 1
            k = unicode(k)
            v = sqlite3.Binary(cPickle.dumps(v))
            c.execute("""INSERT or REPLACE INTO simple ('ckey', 'cvalue') VALUES (?, ?)""", (k, v))
            if p < int(100 * i / l):
                p = int(100 * i / l)
                logger.info("State: " + str(p) + "%")
        source.close()
        os.rename(shelveFilename, shelveFilename + ".old")

    def has_key(self, key):
        try:
            self[key]
            return True
        except KeyError:
            return False

    def __importDataGen(self, iter):
        for k, v in iter:
            k = unicode(k)
            v = cPickle.dumps(v)
            yield (k, v)

    def __createTable(self):
        c = self.__conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS simple(ckey text UNIQUE not NULL, cvalue text)''')

    def __getFromDb(self, key):
        c = self.__conn.cursor()
        c.execute('''SELECT ckey, cvalue FROM simple WHERE ckey = ?''', (key, ))
        try:
            return _readOne(c)[1]
        except StopIteration:
            raise KeyError(key)

    def get(self, k):
        return self[k]

    def __len__(self):
        c = self.__conn.cursor()
        c.execute('''SELECT count(ckey) FROM simple''')
        return c.fetchone()[0]

    def __setToDb(self, key, value, flush=False):
        if not flush:
            self.__batch.append((key, value))
        if len(self.__batch) >= 100:
            self.sync()

    def __putInCache(self, key, value):
        self.__cache[key] = (0, value)
        if len(self.__cache) > self.__maxCacheSize:
            toRemove = []
            for k, v in self.__cache.iteritems():
                toRemove.append((k, v[0]))
            size = len(self.__cache) - self.__minCacheSize
            toRemove = sorted(toRemove, key=lambda x: x[1])[:size]
            logger.info("Remove %d elements from cache" % size)
            for k, c in toRemove:
                del self.__cache[k]

    def __delFromCache(self, key):
        del self.__cache[key]

    def __getFromCache(self, key):
        v = self.__cache[key]
        self.__cache[key] = (v[0] + 1, v[1])
        return v[1]

    def __validateKey(self, key):
        if not isinstance(key, str):
            raise ValueError(u"Wrong type: " + key.__class__.__name__)
        return key

    def items(self):
        iter = self.iteritems()
        for key, value in iter:
            yield key, value

    def iteritems(self):
        c = self.__conn.cursor()
        return Iterator(c)

    def sync(self):
        c = self.__conn.cursor()
        values = []
        for k, v in self.__batch:
            values.append((k, sqlite3.Binary(cPickle.dumps(v))))
        if values:
            logger.debug("Sync values " + str(len(values)))
            c.executemany('''INSERT or REPLACE INTO simple(ckey, cvalue) VALUES(?,?)''', values)
        self.__batch = []

    def close(self):
        logger.info("Close " + self.__sqliteFilename)
        self.sync()
        self.__conn.close()

def open(filename, flags="r", protocol=None):
    return SqliteShelve(filename, flags)


def _simpleTest():
    s = open("/tmp/test.db2", flags="w")
    s["111"] = {"tt": 1, "dd": 2}
    s.sync()
    print "1:" + unicode(s["111"])
    s.close()

    s = open("/tmp/test.db2", flags="w")
    s["222"] = {"tt": 3, "dd": 4}
    s.sync()
    print "2:" + unicode(s["111"])
    print "3:" + unicode(s["222"])
    s.close()

    s = open("/tmp/test.db2", flags="w")
    del s["111"]
    for key, value in s.items():
        print str(key) + " " + str(value)
        #s["3"]
    s.close()

    s = open("/tmp/test.db2", flags="w")
    for i in range(1, 2000):
        s[str(i)] = {"tt": i, "dd": i + 1}
        if i < 100:
            temp = s[str(i)]
    s.close()

    s = open("/tmp/test.db2", flags="w")
    for key, value in s.iteritems():
        print str(key) + " " + str(value)
    s.close()

def _importTest():
    s = open("/tmp/source.db")
    for key, value in s.iteritems():
        print str(key) + " " + str(value)
    s.close()

def _big():
    filename = "/media/eea1ee1d-e5c4-4534-9e0b-24308315e271/pynews/stream/urlResolverCache.db"
    s = open(filename)
    for k, v in s.iteritems():
        logger.info(k)
        logger.info(v)
        break
    s.close()

if __name__ == "__main__":
    #_importTest()
    _simpleTest()
    #_big()