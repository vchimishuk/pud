import os


class SyntaxError(Exception):
    def __init__(self, path, line, msg):
        super().__init__('Syntax error in {}:{}: {}'.format(
            path, line, msg))


class ConfigurationError(Exception):
    pass


class MissingError(ConfigurationError):
    def __init__(self, name):
        super().__init__('Missing required property `{}`.'.format(name))


class TypeError(ConfigurationError):
    def __init__(self, name, value, exp_type):
        msg = 'Property `{}` expected to be {} type but {} found.'.format(
            name, exp_type.__name__, type(value))
        super().__init__(msg)


class Config(dict):
    def __init__(self, path):
        super().__init__()
        self.path = path


def get(config, name, prop_type, default=None):
    if name not in config:
        return default
    if type(config[name]) is not prop_type:
        raise TypeError(name, config[name], prop_type)

    return config[name]


def get_required(config, name, prop_type):
    v = get(config, name, prop_type)
    if v == None:
        raise MissingError(name)

    return v


def isstr(s):
    if len(s) < 3:
        return False

    if not (s.startswith('"') and s.endswith('"')
            or s.startswith("'") and s.endswith("'")):
        return False

    return True


def isnum(s):
    return s.isnumeric()


def isbool(s):
    return s == 'true' or s == 'false'


def parse(path):
    config = Config(path)
    lineno = 0
    with open(path, 'r') as f:
        for l in f:
            lineno += 1
            l = l.strip()
            if not l:
                continue
            if l.startswith('#'):
                continue

            pts = l.split('=', 1)
            if len(pts) != 2:
                msg = 'Invalid line syntax. key=value format expected.'
                raise SyntaxError(path, lineno, msg)

            n, v = pts
            n = n.strip()
            v = v.strip()

            if isstr(v):
                config[n] = v[1:-1]
            elif isnum(v):
                config[n] = int(v)
            elif isbool(v):
                if v == 'true':
                    config[n] = True
                elif v == 'false':
                    config[n] = False
                else:
                    raise AssertionError('Must not happen.')
            else:
                msg = ('Invalid value format. String, '
                       'integer and boolean are supported.')
                raise SyntaxError(path, lineno, msg)

    return config


# TODO: Add configuration name based on conf-file to support
#       multiple instances of the single module.
def load_configs(path):
    configs = []

    for f in os.listdir(path):
        if f.endswith('.conf'):
            configs.append(parse(os.path.join(path, f)))

    return configs
