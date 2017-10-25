##################################################################################
# Module:   args
# Purpose:  Module defining all switches and arguments used by Audit UI
#
# Notes:
#
##################################################################################

import sys
import logging
import data_pipeline.logger.logging_loader
import argparse

logger = logging.getLogger(__name__)

def parse_args(arg_list):
    logger.info("Parsing command line arguments: {}".format(arg_list))

    args_parser = argparse.ArgumentParser()

    args_parser.add_argument("--quiet",           action="store_true", help="quiet mode")
    args_parser.add_argument("--verbose",         action="store_true", help="verbose mode")
    args_parser.add_argument("--veryverbose",     action="store_true", help="very verbose mode")
    args_parser.add_argument("--audituser",       nargs='?', help="process audit user credentials requried for logging processing metrics")
    args_parser.add_argument("--httphost", nargs='?', default = '0.0.0.0', help="process audit web server http host")
    args_parser.add_argument("--httpport", nargs='?', default = '5000', help="process audit web server http port")
    parsed_args = args_parser.parse_args(arg_list)

    return parsed_args


def get_program_args():
    return parse_args(sys.argv[1:])

