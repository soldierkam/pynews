from tweetstream.streamclasses import BaseStream

class SiteStream(BaseStream):
    url = "https://sitestream.twitter.com/1.1/site.json"

    def __init__(self, username, password, follow=None, with_followers=False, replies=False, url=None):
        self._follow = follow
        self._with_followers = with_followers
        self._replies = replies
        BaseStream.__init__(self, username, password, url=url)

    def _get_post_data(self):
        postdata = {}
        if self._follow: postdata["follow"] = ",".join([str(e) for e in self._follow])
        if self._with_followers: postdata["with"] = "followings"
        if self._replies: postdata["replies"] = "all"
        return postdata