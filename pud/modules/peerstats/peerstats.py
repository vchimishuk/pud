# CREATE TABLE torrents (
#     id UInt32,
#     hash FixedString(40),
#     name String,
#     comment String
# )
# ENGINE = MergeTree()
# ORDER BY (id);
#
# CREATE TABLE clients (
#     id UInt32,
#     name String
# )
# ENGINE = MergeTree()
# ORDER BY (id);
#
# CREATE TABLE peers (
#     time DateTime('UTC'),
#     torrent UInt32,
#     ip IPv4,
#     client UInt32,
#     speed UInt32,
#     country FixedString(2),
#     lat Float32,
#     lon Float32
# )
# ENGINE = MergeTree()
# ORDER BY (time);

import time
import json
import collections
import http.client
from clickhouse_driver import Client
import geoip2.database
import pud


Torrent = collections.namedtuple('Torrent', ['hash', 'name', 'comment', 'peers'])
Peer = collections.namedtuple('Peer', ['ip', 'client', 'speed'])
Geo = collections.namedtuple('Geo', ['country', 'lat', 'lon'])


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

        chhost = pud.config.get_required(self.config, 'clickhouse.host', str)
        chport = pud.config.get_required(self.config, 'clickhouse.port', int)
        chdb = pud.config.get_required(self.config, 'clickhouse.db', str)
        self.ch = Client(chhost, chport, chdb)

        geofile = pud.config.get_required(self.config, 'geolite.file', str)
        self.geodb = geoip2.database.Reader(geofile)

        self.clients = self.get_clients()
        self.torrents = self.get_torrents()

    def close(self):
        self.ch.disconnect_connection()
        self.geodb.close()

    @pud.cron('* * * * * */10')
    def update_stats(self):
        now = int(time.time())
        peers = []
        for t in self.tr.get_torrents():
            for p in t.peers:
                geo = self.geoinfo(p.ip)
                peers.append({'time': now,
                              'torrent': self.get_torrent_id(t),
                              'ip': p.ip,
                              'client': self.get_client_id(p.client),
                              'speed': p.speed,
                              'country': geo.country,
                              'lat': geo.lat,
                              'lon': geo.lon})

        q = '''
        INSERT INTO peers (
            time, torrent, ip, client, speed, country, lat, lon
        ) VALUES
        '''
        self.ch.execute(q, peers)

    def get_torrent_id(self, torrent):
        if torrent.hash not in self.torrents:
            id = max(list(self.torrents.values()) + [0]) + 1
            q = 'INSERT INTO torrents (id, hash, name, comment) VALUES'
            self.ch.execute(q, [{'id': id,
                                 'hash': torrent.hash,
                                 'name': torrent.name,
                                 'comment': torrent.comment}])
            self.torrents[torrent.hash] = id

        return self.torrents[torrent.hash]

    def get_client_id(self, client):
        if client not in self.clients:
            id = max(list(self.clients.values()) + [0]) + 1
            q = 'INSERT INTO clients (id, name) VALUES'
            self.ch.execute(q, [{'id': id,
                                 'name': client}])
            self.clients[client] = id

        return self.clients[client]

    def get_clients(self):
        rows = self.ch.execute('SELECT id, name FROM clients')
        cls = {}
        for r in rows:
            cls[r[1]] = r[0]

        return cls

    def get_torrents(self):
        rows = self.ch.execute('SELECT id, hash FROM torrents')
        trs = {}
        for r in rows:
            trs[r[1]] = r[0]

        return trs

    def geoinfo(self, ip):
        try:
            g = self.geodb.city(ip)

            return Geo(g.country.iso_code,
                       g.location.latitude,
                       g.location.longitude)
        except geoip2.errors.AddressNotFoundError:
            return Geo('', 0, 0)
