import pzem
import pyrite
import pud


class Pzem(pud.Module):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dev = pud.config.get_required(self.config, 'dev', str)
        ghost = pud.config.get_required(self.config, 'graphite.host', str)
        gprefix = pud.config.get_required(self.config, 'graphite.prefix', str)

        graphite = pyrite.Pyrite(ghost, 2003, prefix=gprefix, interval=60)
        graphite.gauges('stats', self.stats)

    def close(self):
        self.graphite.close()

    def stats(self):
        pz = pzem.Pzem(self.dev)
        try:
            return ((n, v) for n, v in pz.stats().items())
        finally:
            pz.close()
