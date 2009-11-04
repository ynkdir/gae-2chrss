# coding: utf-8

from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext import db
from google.appengine.api import urlfetch
from google.appengine.api import memcache

import logging
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

def dmemcache(time, keyfmt=None):
    def deco(f):
        def wrap(*args):
            if keyfmt is None:
                key = str(args)
            else:
                key = keyfmt(args)
            v = memcache.get(key, namespace=f.__name__)
            if isinstance(v, Exception):
                raise Exception("CachedError", v)
            if v is None:
                try:
                    v = f(*args)
                    memcache.add(key, v, time=time, namespace=f.__name__)
                except Exception, e:
                    memcache.add(key, e, time=config.error_cache_time, namespace=f.__name__)
                    raise
            else:
                logging.info("cached: " + f.__name__)
            return v
        return wrap
    return deco

def render(template_file, template_values):
    path = os.path.join(os.path.dirname(__file__), "templates", template_file)
    return template.render(path, template_values)

def get_url(url):
    uc = UrlCache.get_by_key_name(url)

    headers = {}
    if uc:
        headers["if-modified-since"] = uc.lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT")
    res = urlfetch.fetch(url=url, headers=headers, follow_redirects=False)

    if "last-modified" in res.headers:
        lastmodified = datetime.datetime.strptime(res.headers["last-modified"], "%a, %d %b %Y %H:%M:%S %Z")
    else:
        lastmodified = datetime.datetime.utcnow()

    if res.status_code == 200:
        uc = UrlCache(key_name=url)
        uc.url = url
        uc.content = db.Blob(res.content)
        uc.lastmodified = lastmodified
        uc.lastaccess = datetime.datetime.utcnow()
        uc.put()
    elif res.status_code == 304:
        pass
    else:
        raise Exception("BadResponse", url, res.status_code)

    return uc

def get_board_title(server, board):
    url = "http://%s/%s/SETTING.TXT" % (server, board)
    uc = get_url(url)
    content = uc.content.decode("cp932", "replace")
    for line in content.splitlines()[1:]:
        key, value = line.split("=", 1)
        if key == "BBS_TITLE":
            title = value
            break
    else:
        title = board
    return title

def parse_dat(server, board, thread, content, limit):
    def linkrepl(m):
        s = m.group(1)
        p = m.group(2)
        if s == 'ttp':
            s = 'http'
        elif s == 'ttps':
            s = 'https'
        return '<a href="%s://%s">%s://%s</a>' % (s, p, m.group(1), p)

    def parse_line(num, line):
        name, mail, dd, body, title = re.split("<>", line)
        body = re.sub(r'(http|ttp|https|ttps|ftp)://([\x21\x23-\x7E]+)', linkrepl, body)
        body = re.sub(r'(<a [^>]*href=")../test/', r'\1http://%s/test/' % server, body)
        m = re.match(r"(?P<year>\d+)/(?P<month>\d+)/(?P<day>\d+)\(.\) (?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)", dd)
        if m:
            try:
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
            except ValueError:
                # It can be an invalid date
                logging.info(dd)
                date = datetime.datetime.utcnow()
        else:
            date = datetime.datetime.utcnow()
        return {
            'num' : num,
            'name' : name,
            'mail' : mail,
            'dd' : dd,
            'date' : date,
            'body' : body,
            'title' : title,
        }

    lines = content.splitlines()
    first = parse_line("1", lines[0])
    return (
        first['title'],
        (parse_line(str(i + 1), lines[i]) for i in range(len(lines) - 1, max(len(lines) - limit, 0) - 1, -1))
    )

@dmemcache(0, lambda args: str(tuple(args[i] for i in (0, 1, 2, 4, 5))))
def dat2atom1(server, board, thread, content, lastmodified, limit):
    title, items = parse_dat(server, board, thread, content, limit)
    f = StringIO.StringIO()
    f.write('<?xml version="1.0" encoding="utf-8"?>')
    f.write('<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="ja">')
    f.write('<title>%s</title>' % title)
    f.write('<author><name></name></author>')
    f.write('<link href="http://%s/test/read.cgi/%s/%s/" />' % (server, board, thread))
    f.write('<id>http://%s/test/read.cgi/%s/%s/</id>' % (server, board, thread))
    f.write('<updated>%s</updated>' % lastmodified.strftime("%Y-%m-%dT%H:%M:%SZ"))
    for item in items:
        f.write('<entry>')
        f.write('<title>%s</title>' % item['num'])
        f.write('<link href="http://%s/test/read.cgi/%s/%s/%s" />' % (server, board, thread, item['num']))
        f.write('<id>http://%s/test/read.cgi/%s/%s/%s</id>' % (server, board, thread, item['num']))
        f.write('<updated>%s</updated>' % item['date'].strftime("%Y-%m-%dT%H:%M:%SZ"))
        f.write('<content type="html"><![CDATA[')
        if item['mail'] == '':
            f.write(u'%s 名前：<b>%s</b> ：%s' % (item['num'], item['name'], item['dd']))
        else:
            f.write(u'%s 名前：<a href="mailto:%s"><b>%s</b></a> ：%s' % (item['num'], item['mail'], item['name'], item['dd']))
        f.write('<p>%s</p>' % item['body'])
        f.write(']]></content>')
        f.write('</entry>')
    f.write('</feed>')
    return f.getvalue().encode('utf-8')

@dmemcache(config.thread_cache_time)
def thread2atom1(server, board, thread, limit):
    url = "http://%s/%s/dat/%s.dat" % (server, board, thread)
    uc = get_url(url)
    rss = dat2atom1(server, board, thread, uc.content.decode("cp932", "replace"), uc.lastmodified, limit)
    return rss

@dmemcache(0, lambda args: str(tuple(args[i] for i in (0, 1, 2, 4, 5))))
def dat2rss2(server, board, thread, content, lastmodified, limit):
    title, items = parse_dat(server, board, thread, content, limit)
    f = StringIO.StringIO()
    f.write('<?xml version="1.0" encoding="utf-8"?>')
    f.write('<rss version="2.0">')
    f.write('<channel>')
    f.write('<title>%s</title>' % title)
    f.write('<link>http://%s/test/read.cgi/%s/%s/</link>' % (server, board, thread))
    f.write('<description>%s</description>' % title)
    f.write('<language>ja</language>')
    f.write('<pubDate>%s</pubDate>' % lastmodified.strftime("%a, %d %b %Y %H:%M:%S GMT"))
    for item in items:
        f.write('<item>')
        f.write('<title>%s</title>' % item['num'])
        f.write('<link>http://%s/test/read.cgi/%s/%s/%s</link>' % (server, board, thread, item['num']))
        f.write('<guid isPermaLink="true">http://%s/test/read.cgi/%s/%s/%s</guid>' % (server, board, thread, item['num']))
        f.write('<pubDate>%s</pubDate>' % item['date'].strftime("%a, %d %b %Y %H:%M:%S GMT"))
        f.write('<description><![CDATA[')
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

@dmemcache(config.thread_cache_time)
def thread2rss2(server, board, thread, limit):
    url = "http://%s/%s/dat/%s.dat" % (server, board, thread)
    uc = get_url(url)
    rss = dat2rss2(server, board, thread, uc.content.decode("cp932", "replace"), uc.lastmodified, limit)
    return rss

def parse_subject(server, board, content, limit):
    def parse_line(line):
        datfile, title = re.split("<>", line)
        thread = re.sub(r"^(\d+)\.dat", r"\1", datfile)
        title = re.sub(r"\s*\(\d+\)$", "", title)
        if thread.startswith("924"):
            # may be a special number for ad.  (e.g. "924%y%m%d", "924%y%m%d1")
            date = None
        else:
            date = datetime.datetime.fromtimestamp(int(thread))
        return {
            "thread" : thread,
            "title" : title,
            "date" : date,
        }
    items = list(parse_line(line) for line in content.splitlines())
    items.sort(key = lambda x: int(x["thread"]), reverse=True)
    return items[ : limit]

@dmemcache(0, lambda args: str(tuple(args[i] for i in (0, 1, 3, 4))))
def subject2atom1(server, board, content, lastmodified, limit):
    items = parse_subject(server, board, content, limit)
    title = get_board_title(server, board)
    f = StringIO.StringIO()
    f.write('<?xml version="1.0" encoding="utf-8"?>')
    f.write('<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="ja">')
    f.write('<title>%s</title>' % title)
    f.write('<author><name></name></author>')
    f.write('<link href="http://%s/%s/" />' % (server, board))
    f.write('<id>http://%s/%s/</id>' % (server, board))
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

@dmemcache(config.board_cache_time)
def board2atom1(server, board, limit):
    url = "http://%s/%s/subject.txt" % (server, board)
    uc = get_url(url)
    rss = subject2atom1(server, board, uc.content.decode("cp932", "replace"), uc.lastmodified, limit)
    return rss

@dmemcache(0, lambda args: str(tuple(args[i] for i in (0, 1, 3, 4))))
def subject2rss2(server, board, content, lastmodified, limit):
    items = parse_subject(server, board, content, limit)
    title = get_board_title(server, board)
    f = StringIO.StringIO()
    f.write('<?xml version="1.0" encoding="utf-8"?>')
    f.write('<rss version="2.0">')
    f.write('<channel>')
    f.write('<title>%s</title>' % title)
    f.write('<link>http://%s/%s/</link>' % (server, board))
    f.write('<description>%s</description>' % title)
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

@dmemcache(config.board_cache_time)
def board2rss2(server, board, limit):
    url = "http://%s/%s/subject.txt" % (server, board)
    uc = get_url(url)
    rss = subject2rss2(server, board, uc.content.decode("cp932", "replace"), uc.lastmodified, limit)
    return rss

class AIndex(webapp.RequestHandler):
    def get(self):
        url = self.request.get("url")
        limit = self.request.get("limit")

        if limit != "" and not re.match(r"^\d{1,4}$", limit):
            raise Exception("ValidationError", limit)

        if url == "":
            template_values = {"root":self.request.url}
            self.response.out.write(render("index.html", template_values))
            return

        m = re.match(r"http://([^/]+)/test/read\.cgi/(\w+)/(\d+)/", url)
        if m:
            server = m.group(1)
            board = m.group(2)
            thread = m.group(3)
            if (not re.match(config.filter_server, server)
                    or not re.match(config.filter_board, board)
                    or not re.match(r"^\d+$", thread)):
                raise Exception("ValidationError", url)
            if limit != "":
                limit = int(limit)
                if limit >= config.thread_limit:
                    limit = config.thread_limit
            else:
                limit = config.thread_limit
            rss = thread2atom1(server, board, thread, limit)
            self.response.headers["content-type"] = "application/atom+xml"
            self.response.out.write(rss)
            return

        m = re.match(r"http://([^/]+)/(\w+)/", url)
        if m:
            server = m.group(1)
            board = m.group(2)
            if (not re.match(config.filter_server, server)
                    or not re.match(config.filter_board, board)):
                raise Exception("ValidationError", url)
            if limit != "":
                limit = int(limit)
                if limit >= config.board_limit:
                    limit = config.board_limit
            else:
                limit = config.board_limit
            rss = board2atom1(server, board, limit)
            self.response.headers["content-type"] = "application/atom+xml"
            self.response.out.write(rss)
            return

        raise Exception("ValidationError", url)

    def handle_exception(self, exception, debug_mode):
        if debug_mode:
            webapp.RequestHandler.handle_exception(self, exception, debug_mode)
        else:
            logging.exception(exception)
            self.error(500)
            self.response.out.write(exception)

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
