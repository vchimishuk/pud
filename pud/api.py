class Module:
    def __init__(self, term, logger, config):
        self.term = term
        self.logger = logger
        self.config = config


# TODO: Add restart flag.
def task(func):
    func.pud_task = True

    return func


def cron(expr):
    def dec(func):
        func.pud_cron = expr

        return func

    return dec
