import re
import psutil
import pyrite
import pud.modules
import pud.config


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
        self.graphite.gauge('uptime', self.uptime)

        mnts = self.mountpoints()
        for n, d in self.config.items():
            m = re.search(r'hdd\.(.+)\.dev', n)
            if m:
                if d not in mnts:
                    self.logger.warn('Mountpoint for %d device not found.', d)
                else:
                    self.graphite.gauge('hdd.{}.total'.format(m.group(1)),
                                        self.gauge_hdd(mnts[d], 'total'))
                    self.graphite.gauge('hdd.{}.used'.format(m.group(1)),
                                        self.gauge_hdd(mnts[d], 'used'))
                    self.graphite.gauge('hdd.{}.free'.format(m.group(1)),
                                        self.gauge_hdd(mnts[d], 'free'))

        for iface in psutil.net_io_counters(True).keys():
            if iface == 'lo':
                continue
            self.graphite.gauge('net.{}.data_rx'.format(iface),
                                self.gauge_net(iface, 'bytes_recv'))
            self.graphite.gauge('net.{}.data_tx'.format(iface),
                                self.gauge_net(iface, 'bytes_sent'))

    def mountpoints(self):
        return {x.device: x.mountpoint for x in psutil.disk_partitions()}

    def uptime(self):
        with open('/proc/uptime', 'r') as f:
            return int(float(f.readline().split()[0]))

    def gauge_hdd(self, dev, metric):
        return lambda: getattr(psutil.disk_usage(dev), metric)

    def gauge_net(self, iface, metric):
        def f():
            c = psutil.net_io_counters(True)
            if iface not in c:
                return None

            return getattr(c[iface], metric)

        return f
