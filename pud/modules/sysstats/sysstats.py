import re
import psutil
import pyrite
import pud.modules
import pud.config


def tuples(obj, names):
    l = []
    for n, p in names.items():
        l.append((n, getattr(obj, p)))

    return l


class SysStats(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        host = pud.config.get_required(self.config, 'graphite.host', str)
        prefix = pud.config.get_required(self.config, 'graphite.prefix', str)
        interval = pud.config.get(self.config, 'graphite.interval', int, 60)
        self.graphite = pyrite.Pyrite(host, 2003,
                                      prefix=prefix,
                                      interval=interval)

        self.register_metrics()

    def close(self):
        self.graphite.close()

    # Periodically check for new devices.
    @pud.cron('*/5 * * * *')
    def register_metrics(self):
        mnts = self.mountpoints()
        for n, d in self.config.items():
            m = re.search(r'hdd\.(.+)\.dev', n)
            if m:
                if d not in mnts:
                    self.logger.warn('Mountpoint for %d device not found.', d)
                else:
                    self.graphite.gauge('hdd.{}'.format(m.group(1)),
                                        self.hdd(mnts[d]))

        for iface in psutil.net_io_counters(True).keys():
            if iface == 'lo':
                continue
            self.graphite.gauges('net.{}'.format(iface), self.net(iface))

        self.graphite.gauges('cpu', self.cpu)
        self.graphite.gauges('mem', self.mem)
        self.graphite.gauge('uptime', self.uptime)

    def mountpoints(self):
        return {x.device: x.mountpoint for x in psutil.disk_partitions()}

    def uptime(self):
        with open('/proc/uptime', 'r') as f:
            return int(float(f.readline().split()[0]))

    def hdd(self, dev):
        def f():
            st = psutil.disk_usage(dev)

            return tuples(st,
                          {'total': 'total',
                           'used': 'used',
                           'free': 'free'})

        return f

    def net(self, iface):
        def f():
            c = psutil.net_io_counters(True)
            if iface not in c:
                return None

            return tuples(c[iface],
                          {'data_rx': 'bytes_recv',
                           'data_tx': 'bytes_sent'})

        return f

    def cpu(self):
        times = tuples(psutil.cpu_times(),
                       {'user': 'user',
                        'system': 'system',
                        'idle': 'idle',
                        'iowait': 'iowait'})
        st = psutil.getloadavg()
        la = [('la1', st[0]), ('la5', st[1]), ('la15', st[2])]

        return times + la

    def mem(self):
        return tuples(psutil.virtual_memory(),
                      {'available': 'available',
                       'buffers': 'buffers',
                       'cached': 'cached',
                       'free': 'free',
                       'shared': 'shared',
                       'total': 'total',
                       'used': 'used'})
