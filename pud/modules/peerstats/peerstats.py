import time
import json
import collections
import http.client
import psycopg2
from psycopg2.extras import NamedTupleCursor
import pud


# CREATE TABLE torrents (
#     id SERIAL PRIMARY KEY,
#     hash VARCHAR(255) NOT NULL,
#     name VARCHAR(255) NOT NULL,
#     comment VARCHAR(255) NOT NULL
# );

# CREATE TABLE clients (
#     id SERIAL PRIMARY KEY,
#     name VARCHAR(255) NOT NULL
# );

# CREATE TABLE peers (
#     id BIGSERIAL PRIMARY KEY,
#     time BIGINT NOT NULL,
#     torrent INT NOT NULL REFERENCES torrents(id),
#     ip VARCHAR(255) NOT NULL,
#     client INT NOT NULL REFERENCES clients(id),
#     speed INT NOT NULL
# );


Torrent = collections.namedtuple('Torrent', ['hash', 'name', 'comment', 'peers'])
Peer = collections.namedtuple('Peer', ['ip', 'client', 'speed'])


class Transmission:
    SID_HEADER = 'X-Transmission-Session-Id'

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sid = ''

    def get_torrents(self):
        resp = self.request('torrent-get',
                            {'ids': 'recently-active',
                             'fields': ['id', 'name', 'comment',
                                        'hashString', 'peers']})
        torrents = []
        for tr in resp['torrents']:
            peers = []
            for p in tr['peers']:
                if p['isUploadingTo']:
                    peers.append(Peer(ip=p['address'],
                                      client=p['clientName'],
                                      speed=p['rateToPeer']))
            if peers:
                torrents.append(Torrent(hash=tr['hashString'],
                                        name=tr['name'],
                                        comment=tr['comment'],
                                        peers=peers))

        return torrents

    def request(self, method, args):
        body = {'method': method,
                'arguments': args}
        retry = 2
        while retry:
            conn = http.client.HTTPConnection(self.host, self.port)
            conn.request('POST', '/rpc', body=json.dumps(body),
                         headers={self.SID_HEADER: self.sid,
                                  'Content-Type': 'application/json'})
            resp = conn.getresponse()
            if resp.status == 200:
                return json.loads(resp.read().decode('utf-8'))['arguments']
            elif resp.status == 409:
                self.sid = resp.getheader(self.SID_HEADER)
                retry -= 1
            else:
                raise IOError('non-OK server response: {}'.format(resp.status))


class PeerStats(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        trhost = pud.config.get_required(self.config, 'transmission.host', str)
        trport = pud.config.get_required(self.config, 'transmission.port', int)
        self.tr = Transmission(trhost, trport)
        pghost = pud.config.get_required(self.config, 'postgres.host', str)
        pgport = pud.config.get_required(self.config, 'postgres.port', int)
        pguser = pud.config.get_required(self.config, 'postgres.user', str)
        pgpass = pud.config.get_required(self.config, 'postgres.pass', str)
        pgdb = pud.config.get_required(self.config, 'postgres.db', str)
        self.conn = psycopg2.connect(dbname=pgdb, user=pguser,
                                     password=pgpass, host=pghost,
                                     port=pgport)
        self.conn.autocommit = True

        self.clients = self.get_clients()
        self.torrents = self.get_torrents()

    @pud.cron('* * * * * */10')
    def update_stats(self):
        now = int(time.time())
        for t in self.tr.get_torrents():
            for p in t.peers:
                self.add_peer(now, t, p)

    def get_clients(self):
        cls = {}
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute('SELECT id, name FROM clients')
            for r in cur:
                cls[r.name] = r.id

        return cls

    def add_client(self, name):
        s = 'INSERT INTO clients (name) VALUES (%(name)s) RETURNING id'
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute(s, {'name': name})
            return cur.fetchone().id

    def get_torrents(self):
        trs = {}
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute('SELECT id, hash FROM torrents')
            for r in cur:
                trs[r.hash] = r.id

        return trs

    def add_torrent(self, torrent):
        s = '''
        INSERT INTO torrents (hash, name, comment)
        VALUES (%(hash)s, %(name)s, %(comment)s) RETURNING id
        '''
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute(s, {'hash': torrent.hash,
                            'name': torrent.name,
                            'comment': torrent.comment})
            return cur.fetchone().id

    def add_peer(self, time, torrent, peer):
        if peer.client not in self.clients:
            self.clients[peer.client] = self.add_client(peer.client)
        if torrent.hash not in self.torrents:
            self.torrents[torrent.hash] = self.add_torrent(torrent)

        s = '''
        INSERT INTO peers (time, torrent, ip, client, speed)
        VALUES (%(time)s, %(torrent)s, %(ip)s, %(client)s, %(speed)s)
        '''
        with self.conn.cursor(cursor_factory=NamedTupleCursor) as cur:
            cur.execute(s, {'time': time,
                            'torrent': self.torrents[torrent.hash],
                            'ip': peer.ip,
                            'client': self.clients[peer.client],
                            'speed': peer.speed})
