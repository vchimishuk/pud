import os
import sys
import time
import signal
import logging
import logging.handlers
import importlib
import threading
import croniter
import pud.modules
import pud.config


CONFIG_DIR = '/etc/pud'
LOGGER_DIR = '/var/log/pud'
LOGGER_FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
LOGGER_LEVEL = logging.DEBUG


def get_logger(mod='pud'):
    file = os.path.join(LOGGER_DIR, mod + '.log')
    h = logging.handlers.RotatingFileHandler(file,
                                             maxBytes=1024 * 1024 * 5,
                                             backupCount=5)
    h.setFormatter(logging.Formatter(LOGGER_FORMAT))
    logger = logging.getLogger(mod)
    logger.addHandler(h)

    return logger


logging.basicConfig(format=LOGGER_FORMAT, level=LOGGER_LEVEL)
logger = get_logger()

term = threading.Event()


class PudError(Exception):
    pass


class RunQueue:
    def __init__(self, crons):
        self.queue = []
        for m, e in crons.items():
            self.queue.append({'method': m,
                               'expr': e,
                               'time': e.get_next()})
        self.sort()

    def next(self):
        r = self.queue[0]['method'], self.queue[0]['time']
        self.queue[0]['time'] = self.queue[0]['expr'].get_next()
        self.sort()

        return r


    def sort(self):
        self.queue.sort(key=lambda x: x['time'])


class TaskThread(threading.Thread):
    def __init__(self, target, *args, **kwargs):
        super().__init__(target=self.withretry(target), daemon=True,
                         *args, **kwargs)

    def withretry(self, target):
        def wrapper(*args, **kwargs):
            while True:
                try:
                    target(*args, **kwargs)
                    break
                except Exception as e:
                    logger.exception('Long task %s failed. Retrying.', target)
                time.sleep(1)

            logger.debug('Long task %s finished successfuly.', target)

        return wrapper


class CronThread(threading.Thread):
    def __inti__(self, *args, **kwargs):
        super().__init__(daemon=True, *args, **kwargs)

    def run(self, *args, **kwargs):
        t = self._target

        try:
            super().run(*args, **kwargs)
        except Exception as e:
            logger.exception('Cron task %s failed.', t)
        else:
            logger.debug('Cron task %s finished succesfuly.', t)


def isexpired(t):
    return time.time() - t > 60


def module(name):
    try:
        importlib.import_module('pud.modules.{}'.format(name))
    except ModuleNotFoundError as e:
        raise PudError(e)

    m = getattr(pud.modules, name)
    if len(m.__all__) != 1:
        raise PudError('Expected 1 exported symbols but {} found'.format(
            len(m.__all__)))

    return getattr(m, m.__all__[0])


def methods(obj):
    ms = []
    for m in dir(obj):
        meth = getattr(obj, m)
        if callable(meth):
            ms.append(meth)

    return ms


def module_tasks(mod):
    tasks = []
    for m in methods(mod):
        if hasattr(m, 'pud_task'):
            tasks.append(m)

    return tasks


def module_crons(mod):
    crons = {}
    for m in methods(mod):
        if hasattr(m, 'pud_cron'):
            crons[m] = m.pud_cron

    return crons


def die(fmt, *args):
    logger.critical(fmt, *args)
    logging.shutdown()
    sys.exit(1)


def on_sigterm(sig, frame):
    logger.info('Got TERM signal. Exiting.')
    term.set()


def run():
    logger.info('Starging.')

    signal.signal(signal.SIGTERM, on_sigterm)

    try:
        cfgs = pud.config.load_configs(os.path.join(CONFIG_DIR, 'modules'))
    except pud.config.SyntaxError as e:
        die('Loading configuration failed: %s', e)

    tasks = []
    crons = {}
    for cfg in cfgs:
        if 'module' not in cfg:
            die('Required `module` property is missing in %s', cfg.path)
        name = cfg['module']
        logger.debug('Initializing %s module.', name)
        try:
            mod_cls = module(name)
            mod = mod_cls(term=term, logger=get_logger(name), config=cfg)
        except (PudError, pud.config.ConfigurationError) as e:
            die('Module %s loading failed: %s', name, e)

        for meth in module_tasks(mod):
            tasks.append(meth)
            logger.debug('Registered %s long task.', meth)

        for meth, expr in module_crons(mod).items():
            try:
                crons[meth] = croniter.croniter(expr)
                logger.debug('Registered %s cron task.', meth)
            except croniter.CroniterBadCronError as e:
                die('Parsing cron expression for %s failed: %s', meth, e)

    running = {}

    for task in tasks:
        logger.info('Executing long task %s', task)
        t = TaskThread(target=task)
        t.start()
        running[task] = t

    if crons:
        runq = RunQueue(crons)

        while not term.is_set():
            meth, runtime = runq.next()
            left = runtime - time.time()

            if left > 0:
                logger.debug('Sleeping for %d seconds till the next run of %s.',
                             left, meth)
                if term.wait(left):
                    break

            for c in crons.keys():
                if c in running and not running[c].is_alive():
                    del running[c]

            if not isexpired(runtime) and meth not in running:
                logger.info('Executing cron task %s', meth)
                t = CronThread(target=meth)
                t.start()
                running[meth] = t
    else:
        term.wait(3153600000)

    for meth, t in running.items():
        if t.is_alive():
            logging.info('Waiting for %s to exit.', meth)
            t.join(5)
            if t.is_alive():
                logging.warn('%s did not exited. Ignoring.', meth)
            else:
                mod = meth.__self__
                if hasattr(mod, 'close'):
                    mod.close()

    logging.shutdown()
