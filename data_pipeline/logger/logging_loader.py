###############################################################################
# Module:    logging_loader
# Purpose:   Loads logging configuration for the logging module
#
# Notes:
#
###############################################################################

import os
import logging.config
import yaml
import data_pipeline.constants.const as const
import data_pipeline.utils.filesystem as filesystem_utils


logfiles = {}


def setup_logging(
    workdirectory,
    filename_prefix=None,
    default_path='conf/logging.yaml',
    default_level=logging.INFO,
    env_key='LOG_CONFIG'
):
    """Setup logging configuration

    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value

    if os.path.exists(path):
        print("Loading logging configuration from file: {}".format(path))
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())

            handlers = config.get(const.HANDLERS, None)
            if handlers:
                for (k, v) in handlers.iteritems():
                    c = v.get(const.CLASS, None)
                    print("setup_logging: k={}, v={}, c={}".format(k, v, c))
                    if c and const.FILE_HANDLER in c:
                        basename_parts = filter(
                            lambda f: f is not None,
                            [filename_prefix, v[const.FILENAME]])

                        joined_basename = "-".join(basename_parts)
                        filename = os.path.join(workdirectory, joined_basename)
                        filesystem_utils.ensure_path_exists(filename)
                        v[const.FILENAME] = filename
                        if k in const.CACHED_HANDLERS:
                            logfiles[k] = filename

            logging.config.dictConfig(config)
            print("Logging config: {}".format(config))
    else:
        print("Loading default logging configuration at default level: {}"
              .format(default_level))
        logging.basicConfig(level=default_level)


def get_logfile(handler_name):
    return logfiles.get(handler_name, const.EMPTY_STRING)
