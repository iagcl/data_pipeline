# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
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

