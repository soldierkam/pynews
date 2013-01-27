import os
from dateutil.parser import parse
import pytz
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, create_session
from sqlalchemy.orm.exc import NoResultFound
from logger import logger

Base = declarative_base()

class UserE(Base):
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True, autoincrement=False)
    name = Column(String(100), nullable=False)
    lang = Column(String(2))
    followersCount = Column(Integer, nullable=False)
    statusesCount = Column(Integer, nullable=False)
    createdAt = Column(Integer, nullable=False)
    tweets = relationship("TweetE", backref="user")
    #cats = relationship("UserCatEntry", backref="user")

    def copy(self):
        v = {}
        v["id"] = self.id
        v["name"] = self.name
        v["followers"] = self.followersCount
        v["statuses"] = self.statusesCount
        v["createdAt"] = self.createdAt
        #v["cats"] = [catEntry.copy() for catEntry in self.cats]
        return v

    def __repr__(self):
        return "<User('%d','%s', '%s', '%s')>" % (self.id, self.name, self.lang, self.followersCount)

    def __eq__(self, other):
        if not isinstance(other, UserE):
            return False
        return self.id == other.id

#class UserCatEntry(Base):
#    __tablename__ = 'user_cats'
#
#    user_id = Column(BigInteger, ForeignKey('users.id'))
#    cat = Column(String(10), nullable=False)
#    count = Column(Integer, nullable=False, default=0)
#
#    def copy(self):
#        return {self.cat: self.count}

association_table = Table('association', Base.metadata,
    Column('tweet_id', Integer, ForeignKey('tweets.id')),
    Column('url_digest', Integer, ForeignKey('urls.digest'))
)

class TweetE(Base):
    __tablename__ = "tweets"

    id = Column(BigInteger, primary_key=True, autoincrement=False)
    retweets = Column(Integer, nullable=False)
    text = Column(String(300), nullable=False)
    createdAt = Column(DateTime(timezone=True), nullable=False)
    user_id = Column(BigInteger, ForeignKey('users.id'))
    urls = relationship("UrlE", secondary=association_table, backref="tweets")

    def copy(self):
        v = {}
        v["id"] = self.id
        v["retweets"] = self.retweets
        v["text"] = self.text
        v["createdAt"] = self.createdAt
        v["user"] = self.user.copy()
        return v

    def __eq__(self, other):
        if not isinstance(other, TweetE):
            return False
        return self.id == other.id

class UrlE(Base):

    __tablename__ = "urls"

    digest = Column(String(40), primary_key=True, autoincrement=False)
    tcoUrl = Column(String(30), nullable=False)
    url = Column(String(512), nullable=False)
    text = Column(Text(), nullable=False)
    title = Column(String(256), nullable=False)
    #html = Column(Text(), nullable=False)
    cat = Column(String(5), nullable=False)
    len = Column(String(10), nullable=False)
    lang = Column(String(2), nullable=False)

    def copy(self):
        v = {}
        v["id"] = self.digest
        v["url"] = self.url
        v["tco"] = self.tcoUrl
        v["text"] = self.text
        v["title"] = self.title
        #v["html"] = self.html
        v["cat"] = self.cat
        v["len"] = self.len
        v["lang"] = self.lang
        v["tweets"] = [t.copy() for t in self.tweets]
        return v

    def __eq__(self, other):
        if not isinstance(other, UrlE):
            return False
        return self.digest == other.digest

class SqlModel():

    def __init__(self, filename, drop=False):
        uri = "sqlite:///" + filename
        logger.info("Open sqlite in " + uri)
        db = create_engine(uri)
        if drop and os.path.exists(filename):
            Base.metadata.drop_all(db)
        Base.metadata.create_all(db)
        self.__session = create_session(bind=db)
        self.__db = db

    def urlExists(self, urlObj):
        try:
            self.__session.query(UrlE).filter(UrlE.digest == urlObj.getRealUrlDigest()).one()
            return True
        except NoResultFound, e:
            return False

    def updateUrl(self, url):
        tweetE = None
        try:
            logger.debug(u"Put " + unicode(url))
            self.__session.begin()
            urlE = self._selectOrCreateUrl(url)
            t = url.tweet()
            tweetE = self._selectOrCreateTweet(t)
            tweetE.urls.append(urlE)
            self.__session.merge(tweetE)
            self.__session.commit()
        except:
            logger.exception("Sql error")
            logger.info(url)
            logger.info(tweetE)
            self.__session.rollback()
            raise BaseException("Error")

    def selectUserWithoutCat(self, limit=10):
        result = self.__db.execute("SELECT u.* FROM users u JOIN tweets t ON t.user_id = u.id LEFT JOIN user_cats uc ON uc.user_id = u.id WHERE uc.user_id is NULL order by t.retweets DESC limit %d", limit)
        return self.__session.query(UserE).instances(result)

    def _selectOrCreateUrl(self, u):
        url = UrlE()
        url.digest = u.getRealUrlDigest()
        for c in u.documentClasses():
            if c in ["short", "medium", "long"]:
                url.len = c
            else:
                url.cat = c
        #url.html = u.getHtml()
        url.lang = u.lang()
        url.tcoUrl = u.getUrl()
        url.text = u.getText()
        url.title = u.getTitle()
        url.url = u.getRealUrl()
        #self.__session.merge(url)
        return url

    def _selectOrCreateTweet(self, t):
        userE = self._selectOrCreateUser(t.user())
        tw = TweetE()
        tw.id = t.id()
        tw.createdAt = self.__parseDate(t.createdAt())
        tw.retweets = t.retweets()
        tw.text = t.text()
        userE.tweets.append(tw)
        return tw

    def _selectOrCreateUser(self, u):
        logger.debug(u"Insert " + unicode(u))
        user = UserE()
        user.id = u.id()
        user.followersCount = u.friendsCount()
        user.lang = u.lang()
        user.name = u.name()
        user.createdAt = self.__parseDate(u.createdAt())
        user.statusesCount = u.statusesCount()
        return user

    def selectUrls(self, cat=None):
        query = self.__session.query(UrlE)
        if cat:
            query = query.filter(UrlE.cat == cat)
        return query.all()

    def __parseDate(self, d):
        return pytz.UTC.normalize(parse(d)).replace(tzinfo=None)#datetime.strptime(t.createdAt(), '%a %b %d %H:%M:%S %z %Y')#Fri Oct 12 08:18:38 +0000 2012'

    def session(self):
        return self.__session

if __name__ == "__main__":
    model = SqlModel("/tmp/test.db")
    model.session().merge(UserE(1, "ssddff", "pl", 12))
    model.session().flush()
    for u in model.session().query(UserE).order_by(UserE.id):
        print u