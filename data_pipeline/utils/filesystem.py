###############################################################################
# Module:    filesystem
# Purpose:   Utility functions for the filesystem
#
# Notes:
#
###############################################################################

import bz2
import glob
import gzip
import os
import time
import data_pipeline.constants.const as const

from os.path import splitext


def open_file(filename, mode):
    """Opens the filename using the given mode and returns the file handle
    Assumes that files ending with .gz are gzip files, similarly for .bz2
    """
    filename_parts = splitext(filename)
    if filename_parts:
        file_ext = filename_parts[-1]
        if file_ext == ".{ext}".format(ext=const.GZ):
            return gzip.open(filename, mode)
        elif file_ext == ".{ext}".format(ext=const.BZ2):
            # python 2.7 bz2 module does not support append mode
            mode = 'w' if mode.startswith('a') else 'r'
            return bz2.BZ2File(filename, mode=mode)
    return open(filename, mode)


def insensitive_glob(pattern):
    """This elegant function comes courtesy of Geoffrey Irving
    https://stackoverflow.com/questions/8151300/ignore-case-in-glob-on-linux
    """
    def either(c):
        return '[%s%s]' % (c.lower(), c.upper()) if c.isalpha() else c
    return glob.glob(''.join(map(either, pattern)))


def ensure_path_exists(filename):
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        os.makedirs(directory)


def append_datetime_dir(dirpath):
    """Returns the fully qualified work directory by
       appending a datetime formatted string to the given
       base workdirectory
    """
    return os.path.join(dirpath, time.strftime('%Y%m%d_%H%M%S'))
