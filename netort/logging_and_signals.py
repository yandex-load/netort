import logging
import sys


class SingleLevelFilter(logging.Filter):
    """Exclude or approve one msg type at a time.    """

    def __init__(self, passlevel, reject):
        logging.Filter.__init__(self)
        self.passlevel = passlevel
        self.reject = reject

    def filter(self, record):
        if self.reject:
            return record.levelno != self.passlevel
        else:
            return record.levelno == self.passlevel


def init_logging(log_filename, verbose, quiet):
    """ Set up logging, as it is very important for console tool """
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    # create file handler which logs even debug messages
    if log_filename:
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s"
            ))
        logger.addHandler(file_handler)

    # create console handler with a higher log level
    console_handler = logging.StreamHandler(sys.stdout)
    stderr_hdl = logging.StreamHandler(sys.stderr)

    fmt_verbose = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s %(filename)s:%(lineno)d\t%(message)s"
    )
    fmt_regular = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")

    if verbose:
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(fmt_verbose)
        stderr_hdl.setFormatter(fmt_verbose)
    elif quiet:
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(fmt_regular)
        stderr_hdl.setFormatter(fmt_regular)
    else:
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(fmt_regular)
        stderr_hdl.setFormatter(fmt_regular)

    f_err = SingleLevelFilter(logging.ERROR, True)
    f_warn = SingleLevelFilter(logging.WARNING, True)
    f_crit = SingleLevelFilter(logging.CRITICAL, True)
    console_handler.addFilter(f_err)
    console_handler.addFilter(f_warn)
    console_handler.addFilter(f_crit)
    logger.addHandler(console_handler)

    f_info = SingleLevelFilter(logging.INFO, True)
    f_debug = SingleLevelFilter(logging.DEBUG, True)
    stderr_hdl.addFilter(f_info)
    stderr_hdl.addFilter(f_debug)
    logger.addHandler(stderr_hdl)
