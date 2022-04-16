import threading
import pygraphite
import transmission_rpc
import pud.modules
import pud.config


class Transmission(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ghost = pud.config.get_required(self.config, 'graphite.host', str)
        gprefix = pud.config.get_required(self.config, 'graphite.prefix', str)
        ginterval = pud.config.get(self.config, 'graphite.interval', int, 60)
        self.graphite = pygraphite.Graphite(ghost, 2003,
                                            prefix=gprefix,
                                            interval=ginterval)
        self.host = pud.config.get_required(self.config, 'transmission.host', str)
        self.port = pud.config.get_required(self.config, 'transmission.port', int)

        self.stats = {}
        self.statsmu = threading.Lock()

    def close(self):
        self.graphite.close()

    @pud.cron('* * * * *')
    def update_stats(self):
        st = self.get_stats()
        with self.statsmu:
            self.stats = st

        self.register_gauge('speed_rx')
        self.register_gauge('speed_tx')
        self.register_gauge('data_rx')
        self.register_gauge('data_tx')
        self.register_gauge('torrents_total')
        self.register_gauge('torrents_active')

    def get_stats(self):
        try:
            tr = transmission_rpc.Client(host=self.host, port=self.port)
            stats = dict(tr.session_stats().items())

            return {'speed_rx': stats['downloadSpeed'],
                    'speed_tx': stats['uploadSpeed'],
                    'data_rx': stats['cumulative_stats']['downloadedBytes'],
                    'data_tx': stats['cumulative_stats']['uploadedBytes'],
                    'torrents_total': stats['torrentCount'],
                    'torrents_active': stats['activeTorrentCount']}
        except transmission_rpc.TransmissionError as e:
            raise Exception(e) from None

    def register_gauge(self, name):
        def func():
            with self.statsmu:
                return self.stats[name]

        self.graphite.gauge(name, func)
