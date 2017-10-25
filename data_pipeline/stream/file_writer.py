###############################################################################
# Module:    file_writer
# Purpose:   Utility to write newline separated records into a file
#
# Notes:
#
###############################################################################

import logging
import data_pipeline.utils.filesystem as fsutils

from .stream_writer import StreamWriter


class FileWriter(StreamWriter):
    def __init__(self, filename, mode='a'):
        self._logger = logging.getLogger(__name__)
        self._filename = str(filename)
        self.handle = None
        self._init_file_handle(filename, mode)

    def _init_file_handle(self, filename, mode):
        if not filename:
            error_message = "Output filename was not defined"
            raise ValueError()

        self.handle = fsutils.open_file(filename, mode)
        self._logger.info("Opened file for writing in mode "
                          "'{mode}': {filename}"
                          .format(mode=mode,
                                  filename=filename))

    def write(self, line):
        if self.handle:
            self.handle.write("{line}".format(line=line))

    def writeln(self, line):
        self.write("{}\n".format(line))

    def flush(self):
        if self.handle and not self.handle.closed:
            # BZ2File doesn't expose a flush() function
            try:
                self.handle.flush()
            except:
                pass

    def close(self):
        if self.handle and not self.handle.closed:
            self.handle.close()

    def __del__(self):
        self.flush()
        self.close()
