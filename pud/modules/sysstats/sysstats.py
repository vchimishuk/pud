import re
import psutil
import pygraphite
import pud.modules
import pud.config


class SysStats(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        host = pud.config.get_required(self.config, 'graphite.host', str)
        prefix = pud.config.get_required(self.config, 'graphite.prefix', str)
        interval = pud.config.get(self.config, 'graphite.interval', int, 60)
        self.graphite = pygraphite.Graphite(host, 2003,
                                            prefix=prefix,
                                            interval=interval)

        self.graphite.gauge('uptime', self.uptime)

        mnts = self.mountpoints()
        for n, d in self.config.items():
            m = re.search(r'hdd\.(.+)\.dev', n)
            if m:
                if d not in mnts:
                    raise ValueError('device not found: ' + d)
                self.graphite.gauge('hdd.{}.total'.format(m.group(1)),
                                    self.gauge_hdd_total(mnts[d]))
                self.graphite.gauge('hdd.{}.used'.format(m.group(1)),
                                    self.gauge_hdd_used(mnts[d]))
                self.graphite.gauge('hdd.{}.free'.format(m.group(1)),
                                    self.gauge_hdd_free(mnts[d]))

    def close(self):
        self.graphite.close()

    def mountpoints(self):
        return {x.device: x.mountpoint for x in psutil.disk_partitions()}

    def uptime(self):
        with open('/proc/uptime', 'r') as f:
            return int(float(f.readline().split()[0]))

    def gauge_hdd_total(self, dev):
        return lambda: psutil.disk_usage(dev).total

    def gauge_hdd_used(self, dev):
        return lambda: psutil.disk_usage(dev).used

    def gauge_hdd_free(self, dev):
        return lambda: psutil.disk_usage(dev).free
