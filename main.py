# coding: utf-8

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
import StringIO

import config

class UrlCache(db.Model):
    url = db.StringProperty()
    content = db.BlobProperty()
    lastmodified = db.DateTimeProperty()
    lastaccess = db.DateTimeProperty()
    rss = db.BlobProperty()

def render(template_file, template_values):
    path = os.path.join(os.path.dirname(__file__), "templates", template_file)
    return template.render(path, template_values)

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

class Thread2Rss:
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
        return rss

    def dat2rss(self, server, board, thread, content, lastmodified):
        items = list(self.parse(content, server))
        title = items[0]['title']
        items.reverse()
        if config.thread_max_items > 0:
            items = items[ : config.thread_max_items]
        return self.render(server, board, thread, items, lastmodified, title)

    def parse(self, content, server):
        def linkrepl(m):
            s = m.group(1)
            p = m.group(2)
            if s == 'ttp':
                s = 'http'
            elif s == 'ttps':
                s = 'https'
            return '<a href="%s://%s">%s://%s</a>' % (s, p, m.group(1), p)

        for i, line in enumerate(content.splitlines()):
            num = str(i + 1)
            name, mail, dd, body, title = re.split("<>", line)
            body = re.sub(r'(http|ttp|https|ttps|ftp)://([\x21\x23-\x7E]+)', linkrepl, body)
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

class Thread2Rss2(Thread2Rss):
    def content_type(self):
        return "application/rss+xml"

    def render(self, server, board, thread, items, lastmodified, title):
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

class Thread2Atom1(Thread2Rss):
    def content_type(self):
        return "application/atom+xml"

    def render(self, server, board, thread, items, lastmodified, title):
        f = StringIO.StringIO()
        f.write('<?xml version="1.0" encoding="utf-8"?>')
        f.write('<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="ja">')
        f.write('<title>2ch: %s</title>' % title)
        f.write('<author><name></name></author>')
        f.write('<link href="http://%s/test/read.cgi/%s/%s/" />' % (server, board, thread))
        f.write('<updated>%s</updated>' % lastmodified.strftime("%Y-%m-%dT%H:%M:%SZ"))
        for item in items:
            f.write('<entry>')
            f.write('<title>%s</title>' % item['num'])
            f.write('<link href="http://%s/test/read.cgi/%s/%s/%s" />' % (server, board, thread, item['num']))
            f.write('<id>http://%s/test/read.cgi/%s/%s/%s</id>' % (server, board, thread, item['num']))
            f.write('<updated>%s</updated>' % item['date'].strftime("%Y-%m-%dT%H:%M:%SZ"))
            f.write('<content type="html"><![CDATA[')
            if config.thread_show_head:
                if item['mail'] == '':
                    f.write(u'%s 名前：<b>%s</b> ：%s' % (item['num'], item['name'], item['dd']))
                else:
                    f.write(u'%s 名前：<a href="mailto:%s"><b>%s</b></a> ：%s' % (item['num'], item['mail'], item['name'], item['dd']))
            f.write('<p>%s</p>' % item['body'])
            f.write(']]></content>')
            f.write('</entry>')
        f.write('</feed>')
        return f.getvalue().encode('utf-8')

class Board2Rss:
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
        return rss

    def subject2rss(self, server, board, content, lastmodified):
        items = list(self.parse(content))
        items.sort(key = lambda x: int(x["thread"]), reverse=True)
        if config.board_max_items > 0:
            items = items[ : config.board_max_items]
        return self.render(server, board, items, lastmodified)

    def parse(self, content):
        for line in content.splitlines():
            datfile, title = re.split("<>", line)
            thread = re.sub(r"^(\d+)\.dat", r"\1", datfile)
            title = re.sub(r"\s*\(\d+\)$", "", title)
            if thread.startswith("924"):
                # may be a special number for ad.  (e.g. "924%y%m%d", "924%y%m%d1")
                date = None
            else:
                date = datetime.datetime.fromtimestamp(int(thread))
            yield {
                "thread" : thread,
                "title" : title,
                "date" : date,
            }

class Board2Rss2(Board2Rss):
    def content_type(self):
        return "application/rss+xml"

    def render(self, server, board, items, lastmodified):
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
            if item['date']:
                f.write('<pubDate>%s</pubDate>' % item['date'].strftime("%a, %d %b %Y %H:%M:%S GMT"))
            f.write('</item>')
        f.write('</channel>')
        f.write('</rss>')
        return f.getvalue().encode('utf-8')

class Board2Atom1(Board2Rss):
    def content_type(self):
        return "application/atom+xml"

    def render(self, server, board, items, lastmodified):
        f = StringIO.StringIO()
        f.write('<?xml version="1.0" encoding="utf-8"?>')
        f.write('<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="ja">')
        f.write('<title>2ch: %s</title>' % board)
        f.write('<author><name></name></author>')
        f.write('<link href="http://%s/test/read.cgi/%s/" />' % (server, board))
        f.write('<updated>%s</updated>' % lastmodified.strftime("%Y-%m-%dT%H:%M:%SZ"))
        for item in items:
            f.write('<entry>')
            f.write('<title>%s</title>' % item['title'])
            f.write('<link href="http://%s/test/read.cgi/%s/%s/" />' % (server, board, item['thread']))
            f.write('<id>http://%s/test/read.cgi/%s/%s/</id>' % (server, board, item['thread']))
            if item['date']:
                f.write('<updated>%s</updated>' % item['date'].strftime("%Y-%m-%dT%H:%M:%SZ"))
            f.write('</entry>')
        f.write('</feed>')
        return f.getvalue().encode('utf-8')

class AIndex(webapp.RequestHandler):
    def get(self):
        url = self.request.get("url")

        if url == "":
            template_values = {"root":self.request.url}
            self.response.out.write(render("index.html", template_values))
            return

        m = re.match(r"http://([^\/]+)/test/read\.cgi/(\w+)/(\d+)/", url)
        if m:
            c = Thread2Atom1()
            rss = c.get(m.group(1), m.group(2), m.group(3))
            self.response.headers["Content-Type"] = c.content_type()
            self.response.out.write(rss)
            return

        m = re.match(r"http://([^\/]+)/(\w+)/", url)
        if m:
            c = Board2Atom1()
            rss = c.get(m.group(1), m.group(2))
            self.response.headers["Content-Type"] = c.content_type()
            self.response.out.write(rss)
            return

        raise Exception("Validate")

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

application = webapp.WSGIApplication(
    [('/', AIndex),
     ('/clean', AClean),
    ],
    debug=config.debug)

def main():
    run_wsgi_app(application)

if __name__ == "__main__":
    main()
