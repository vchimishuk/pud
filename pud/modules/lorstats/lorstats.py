import re
import time
import datetime
import threading
import http.client
import sqlite3
import pyrite
import pud


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.score = 0
        self.cmnts = 0

    def getscore(self):
        with self.lock:
            return self.score

    def setscore(self, score):
        with self.lock:
            self.score = score

    def getcomments(self):
        with self.lock:
            return self.cmnts

    def setcomments(self, cmnts):
        with self.lock:
            self.cmnts = cmnts


class LorStats(pud.Module):
    UA = 'Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cookie = pud.config.get_required(self.config, 'cookie', str)
        self.dbpath = pud.config.get_required(self.config, 'db', str)

        ghost = pud.config.get_required(self.config, 'graphite.host', str)
        gprefix = pud.config.get_required(self.config, 'graphite.prefix', str)
        self.graphite = pyrite.Pyrite(ghost, 2003, prefix=gprefix)

        self.st = Stats()
        self.dbexec('''
            CREATE TABLE IF NOT EXISTS stats (
            time INT PRIMARY KEY,
            score INT,
            comments INT
        )
        ''')

    def close(self):
        self.graphite.close()

    @pud.cron('0 * * * *')
    def stats(self):
        p = self.getprofile()
        self.st.setscore(self.getscore(p))
        self.st.setcomments(self.getcomments(p))

        sql = ('INSERT INTO stats (time, score, comments) '
               'VALUES (?, ?, ?)')
        self.dbexec(sql, (int(datetime.datetime.utcnow().timestamp()),
                          self.st.getscore(),
                          self.st.getcomments()))

        self.graphite.gauge('score', self.st.getscore)
        self.graphite.gauge('comments', self.st.getcomments)
        self.graphite.counter('update').inc()

    def dbexec(self, sql, params=()):
        conn = sqlite3.connect(self.dbpath)
        try:
            with conn:
                conn.execute(sql, params)
        finally:
            conn.close()

    def getprofile(self):
        conn = http.client.HTTPSConnection('www.linux.org.ru', 443)
        conn.request('GET', '/people/urxvt/profile',
                     headers={'User-Agent': self.UA, 'Cookie': self.cookie})

        resp = conn.getresponse()
        if resp.status != 200:
            raise IOError('non-OK server response: {}'.format(resp.status))

        return resp.read().decode('utf-8')

    def getscore(self, profile):
        m = re.search('<b>Score:</b> (\d+)<br>', profile)
        if not m:
            raise IOError('score not found')

        return int(m.group(1))

    def getcomments(self, profile):
        m = re.search('<b>Число комментариев:</b> (\d+)<p>', profile)
        if not m:
            raise IOError('comments not found')

        return int(m.group(1))
