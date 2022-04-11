import pygraphite
import pud.modules
import pud.config


class SysStats(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ghost = pud.config.get_required(self.config, 'graphite.host', str)
        gprefix = pud.config.get_required(self.config, 'graphite.prefix', str)
        self.graphite = pygraphite.Graphite(ghost, 2003, prefix=gprefix)

        self.graphite.gauge('uptime', self.uptime)

    def close(self):
        self.graphite.close()

    def uptime(self):
        with open('/proc/uptime', 'r') as f:
            return int(float(f.readline().split()[0]))
