import sys
import logging
import json
import numpy as np
import string
import re
import uuid


def configure_logging(verbosity):
    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "ERROR")
        logging.addLevelName(logging.WARNING, "WARNING")
        logging.addLevelName(logging.INFO, "INFO")
        logging.addLevelName(logging.DEBUG, "DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]
    logging.getLogger().setLevel(level)


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def make_cromwell_compat_string(s):
    regex = re.compile('[%s]' % re.escape(string.punctuation))
    return regex.sub('-', s)


def fix_url(http):
    return "http://" + http.rpartition('/')[-1]


def get_unique_id():
    return str(uuid.uuid1())[0:7]


def get_dict_val(my_dict, key, err_msg):
    try:
        return my_dict[key]
    except KeyError:
        logging.error(err_msg)
        raise KeyError(err_msg)
