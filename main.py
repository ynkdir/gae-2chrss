# coding: utf-8

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
from google.appengine.api import urlfetch
from google.appengine.api import memcache

import re
import os
import os.path
import datetime
import StringIO

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
        uc.rss = db.Blob(f_2rss(uc))
        uc.put()
    elif res.status_code == 304:
        pass
    else:
        raise Exception("BadResponse")

    return uc

class AClean(webapp.RequestHandler):
    def get(self):
        if self.request.get('all') != '':
            memcache.flush_all()
            for uc in UrlCache.all():
                uc.delete()
        else:
            d = datetime.datetime.utcnow() - datetime.timedelta(seconds=config.clean_time)
            for r in db.GqlQuery("SELECT * FROM UrlCache WHERE lastaccess < :1", d):
                r.delete()

class AThreadRss(webapp.RequestHandler):
    def get(self, server, board, thread):
        if (not re.match(config.filter_server, server)
                or not re.match(config.filter_board, board)
                or not re.match(r"^\d+$", thread)):
            raise Exception("Validate")
        url = "http://%s/%s/dat/%s.dat" % (server, board, thread)
        rss = memcache.get(url)
        if rss == "error":
            raise Exception("CachedError")
        if rss is None:
            try:
                f_2rss = lambda uc: self.dat2rss(server, board, thread, uc.content.decode("cp932"), uc.lastmodified)
                uc = geturl(url, f_2rss)
                rss = uc.rss
                memcache.add(url, rss, time=config.thread_cache_time)
            except:
                memcache.add(url, "error", time=config.error_cache_time)
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
                name, mail, dd, body, title = re.split("<>", line)
                body = re.sub(r'(http|ttp|tp|https|ttps|tps|ftp)://([\x21\x23-\x7E]+)', linkrepl, body)
                body = re.sub(r'(<a [^>]*href=")../test/', r'\1http://%s/test/' % server, body)
                m = re.match(r"(?P<year>\d+)/(?P<month>\d+)/(?P<day>\d+)\(.\) (?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)", dd)
                if m:
                    date = datetime.datetime(
                        year = int(m.group("year")),
                        month = int(m.group("month")),
                        day = int(m.group("day")),
                        hour = int(m.group("hour")),
                        minute = int(m.group("minute")),
                        second = int(m.group("second"))
                    )
                    # JST -> GMT
                    date -= datetime.timedelta(hours=9)
                else:
                    date = datetime.datetime.utcnow()
                yield {
                    'num' : num,
                    'name' : name,
                    'mail' : mail,
                    'dd' : dd,
                    'date' : date,
                    'body' : body,
                    'title' : title,
                }

        items = list(parse())
        title = items[0]['title']
        items.reverse()
        if config.thread_max_items > 0:
            items = items[ : config.thread_max_items]

        f = StringIO.StringIO()
        f.write('<?xml version="1.0" encoding="utf-8"?>')
        f.write('<rss version="2.0">')
        f.write('<channel>')
        f.write('<title>2ch: %s</title>' % title)
        f.write('<link>http://%s/test/read.cgi/%s/%s/</link>' % (server, board, thread))
        f.write('<description>2ch: %s</description>' % title)
        f.write('<language>ja</language>')
        f.write('<pubDate>%s</pubDate>' % lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT"))
        for item in items:
            f.write('<item>')
            f.write('<title>%s</title>' % item['num'])
            f.write('<link>http://%s/test/read.cgi/%s/%s/%s</link>' % (server, board, thread, item['num']))
            f.write('<guid isPermaLink="true">http://%s/test/read.cgi/%s/%s/%s</guid>' % (server, board, thread, item['num']))
            f.write('<pubDate>%s</pubDate>' % item['date'].strftime("%a, %d %b %Y %H:%M:%S GMT"))
            f.write('<description><![CDATA[')
            if config.thread_show_head:
                if item['mail'] == '':
                    f.write(u'%s 名前：<b>%s</b> ：%s' % (item['num'], item['name'], item['dd']))
                else:
                    f.write(u'%s 名前：<a href="mailto:%s"><b>%s</b></a> ：%s' % (item['num'], item['mail'], item['name'], item['dd']))
            f.write('<p>%s</p>' % item['body'])
            f.write(']]></description>')
            f.write('</item>')
        f.write('</channel>')
        f.write('</rss>')
        return f.getvalue().encode('utf-8')

class ABoardRss(webapp.RequestHandler):
    def get(self, server, board):
        if (not re.match(config.filter_server, server)
                or not re.match(config.filter_board, board)):
            raise Exception("Validate")
        url = "http://%s/%s/subject.txt" % (server, board)
        rss = memcache.get(url)
        if rss == "error":
            raise Exception("CachedError")
        if rss is None:
            try:
                f_2rss = lambda uc: self.subject2rss(server, board, uc.content.decode("cp932"), uc.lastmodified)
                uc = geturl(url, f_2rss)
                rss = uc.rss
                memcache.add(url, rss, time=config.board_cache_time)
            except:
                memcache.add(url, "error", time=config.error_cache_time)
                raise
        self.response.headers["Content-Type"] = "application/rss+xml"
        self.response.out.write(rss)

    def subject2rss(self, server, board, content, lastmodified):
        def parse():
            for line in content.splitlines():
                datfile, title = re.split("<>", line)
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

        f = StringIO.StringIO()
        f.write('<?xml version="1.0" encoding="utf-8"?>')
        f.write('<rss version="2.0">')
        f.write('<channel>')
        f.write('<title>2ch: %s</title>' % board)
        f.write('<link>http://%s/test/read.cgi/%s/</link>' % (server, board))
        f.write('<description>2ch: %s</description>' % board)
        f.write('<language>ja</language>')
        f.write('<pubDate>%s</pubDate>' % lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT"))
        for item in items:
            f.write('<item>')
            f.write('<title>%s</title>' % item['title'])
            f.write('<link>http://%s/test/read.cgi/%s/%s/</link>' % (server, board, item['thread']))
            f.write('<guid isPermaLink="true">http://%s/test/read.cgi/%s/%s/</guid>' % (server, board, item['thread']))
            f.write('</item>')
        f.write('</channel>')
        f.write('</rss>')
        return f.getvalue().encode('utf-8')

application = webapp.WSGIApplication(
    [('/clean', AClean),
     ('/(.+)/(.+)/(.+)/', AThreadRss),
     ('/(.+)/(.+)/', ABoardRss),
    ],
    debug=config.debug)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
