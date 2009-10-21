from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
from google.appengine.api import urlfetch
from google.appengine.api import memcache

import re
import os
import os.path
import datetime
from xml.sax.saxutils import unescape

import config

class UrlCache(db.Model):
    url = db.StringProperty()
    content = db.BlobProperty()
    lastmodified = db.DateTimeProperty()
    lastaccess = db.DateTimeProperty()
    rss = db.BlobProperty()

def geturl(url, f_2rss):
    key_name = "uc_" + url
    uc = UrlCache.get_by_key_name(key_name)

    headers = {}
    if uc:
        headers["if-modified-since"] = uc.lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT")
    res = urlfetch.fetch(url=url, headers=headers, follow_redirects=False)

    if "last-modified" in res.headers:
        lastmodified = datetime.datetime.strptime(res.headers["last-modified"], "%a, %d %b %Y %H:%M:%S %Z")
    else:
        lastmodified = datetime.datetime.utcnow()

    if res.status_code == 200:
        uc = UrlCache(key_name=key_name)
        uc.url = url
        uc.content = db.Blob(res.content)
        uc.lastmodified = lastmodified
        uc.lastaccess = datetime.datetime.utcnow()
        uc.rss = f_2rss(uc)
        uc.put()
    elif res.status_code == 304 and uc:
        uc.lastaccess = datetime.datetime.utcnow()
        uc.put()
    else:
        raise Exception("BadResponse")

    return uc

def render(template_file, template_values):
    path = os.path.join(os.path.dirname(__file__), "templates", template_file)
    return template.render(path, template_values)

class AIndex(webapp.RequestHandler):
    def get(self):
        template_values = {'root': self.request.url}
        self.response.out.write(render("index.html", template_values))

class AClean(webapp.RequestHandler):
    def get(self):
        d = datetime.datetime.utcnow() - datetime.timedelta(seconds=config.clean_time)
        q = db.GqlQuery("SELECT * FROM UrlCache WHERE lastaccess < :1", d)
        for r in q.fetch(1000):
            r.delete()

class AThreadRss(webapp.RequestHandler):
    def get(self, server, board, thread):
        if (not re.match(config.filter_server, server)
                or not re.match(config.filter_board, board)
                or not re.match(r"^\d+$", thread)):
            raise Exception("Validate")
        key = "%s%s%s" % (server, board, thread)
        rss = memcache.get(key)
        if rss == "error":
            raise Exception("CachedError")
        if rss is None:
            try:
                url = "http://%s/%s/dat/%s.dat" % (server, board, thread)
                f_2rss = lambda uc: self.dat2rss(server, board, thread, uc.content.decode("cp932"), uc.lastmodified)
                uc = geturl(url, f_2rss)
                rss = uc.rss
                memcache.add(key, rss, time=config.thread_cache_time)
            except:
                memcache.add(key, "error", time=config.error_cache_time)
                raise
        self.response.headers["Content-Type"] = "application/rss+xml"
        self.response.out.write(rss)

    def dat2rss(self, server, board, thread, content, lastmodified):
        def linkrepl(m):
            s = m.group(1)
            p = m.group(2)
            if s == 'ttp' or s == 'tp':
                s = 'http'
            elif s == 'ttps' or s == 'tps':
                s = 'https'
            return '<a href="%s://%s">%s://%s</a>' % (s, p, m.group(1), p)

        def parse():
            for i, line in enumerate(content.splitlines()):
                num = str(i + 1)
                name, mail, dd, body, title = [unescape(x) for x in re.split("<>", line)]
                body = re.sub(r'(?<!<a href=")(h?t?tps?|ftp)://([!-~]+)', linkrepl, body)
                body = body.replace('<a href="../test/', '<a href="http://%s/test/' % server)
                if dd == "":
                    date = datetime.datetime.utcnow.strftime("%a, %d %b %Y %H:%M:%S GMT")
                    id = None
                    be = None
                else:
                    m = re.match(r"(?P<year>\d+)/(?P<month>\d+)/(?P<day>\d+)\(.\) (?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)(?: ID:(?P<id>\S+))?(?: BE:(?P<be>\S+))?", dd)
                    d = datetime.datetime(
                        year = int(m.group("year")),
                        month = int(m.group("month")),
                        day = int(m.group("day")),
                        hour = int(m.group("hour")),
                        minute = int(m.group("minute")),
                        second = int(m.group("second"))
                    )
                    # JST -> GMT
                    d = d - datetime.timedelta(hours=9)
                    date = d.strftime("%a, %d %b %Y %H:%M:%S GMT")
                    id = m.group("id")
                    be = m.group("be")
                yield {
                    'num' : num,
                    'name' : name,
                    'mail' : mail,
                    'dd' : dd,
                    'date' : date,
                    'id' : id,
                    'be' : be,
                    'body' : body,
                    'title' : title,
                }

        items = list(parse())
        title = items[0]['title']
        items.reverse()
        if config.thread_max_items > 0:
            items = items[ : config.thread_max_items]
        template_values = {
            'config' : config,
            'server' : server,
            'board' : board,
            'thread' : thread,
            'title' : title,
            'lastmodified' : lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            'items' : items,
        }
        return render("thread.rss", template_values)


class ABoardRss(webapp.RequestHandler):
    def get(self, server, board):
        if (not re.match(config.filter_server, server)
                or not re.match(config.filter_board, board)):
            raise Exception("Validate")
        key = "%s%s" % (server, board)
        rss = memcache.get(key)
        if rss == "error":
            raise Exception("CachedError")
        if rss is None:
            try:
                url = "http://%s/%s/subject.txt" % (server, board)
                f_2rss = lambda uc: self.subject2rss(server, board, uc.content.decode("cp932"), uc.lastmodified)
                uc = geturl(url, f_2rss)
                rss = uc.rss
                memcache.add(key, rss, time=config.board_cache_time)
            except:
                memcache.add(key, "error", time=config.error_cache_time)
                raise
        self.response.headers["Content-Type"] = "application/rss+xml"
        self.response.out.write(rss)

    def subject2rss(self, server, board, content, lastmodified):
        def parse():
            for line in content.splitlines():
                datfile, title = [unescape(x) for x in re.split("<>", line)]
                thread = re.sub(r"^(\d+)\.dat", r"\1", datfile)
                title = re.sub(r"\s*\(\d+\)$", "", title)
                yield {
                    "thread" : thread,
                    "title" : title,
                }
        items = list(parse())
        items.sort(key = lambda x: int(x["thread"]), reverse=True)
        if config.board_max_items > 0:
            items = items[ : config.board_max_items]
        template_values = {
            'config' : config,
            'server' : server,
            'board' : board,
            'lastmodified' : lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            'items' : items,
        }
        return render("board.rss", template_values)

application = webapp.WSGIApplication(
    [('/', AIndex),
     ('/clean', AClean),
     ('/(.+)/(.+)/(.+)/', AThreadRss),
     ('/(.+)/(.+)/', ABoardRss),
    ],
    debug=config.debug)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
