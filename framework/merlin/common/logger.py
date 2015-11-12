#
# Copyright (c) 2015 EPAM Systems, Inc. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
# Redistributions in binary form must reproduce the above copyright notice, this
# list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
# Neither the name of the EPAM Systems, Inc. nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# See the NOTICE file and the LICENSE file distributed with this work
# for additional information regarding copyright ownership and licensing.
#

"""
Logger for all job

"""
import logging
import logging.config
import argparse
import os
import sys

# Default logging configuration
DEFAULT_LOG_CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'resources', 'logging.conf')
__ready_to_use__ = False


def get_logger(name=None):
    """
    Returns a logger with the specified name, creating it if necessary.
    If no name is specified, returns the root logger.
    :type name: str
    """
    __configure_logger()
    return logging.getLogger(name)


def __configure_logger():
    """
    Reads the logging configuration from a external config file.
    Use --log-conf <path> command line parameter to inject logging configuration.
    If configuration file was not provided then
    the default configuration file at 'resources/logging.conf' will be used.
    """
    global __ready_to_use__
    if not __ready_to_use__:
        parser = argparse.ArgumentParser(description='Configure framework logger')
        args, script = parser.parse_known_args(sys.argv)
        logging_conf = os.path.join(os.path.dirname(script[0]), 'resources', 'logging.conf')
        if os.path.isfile(logging_conf):
            parser.add_argument('--log-conf',
                                help='logger configuration file',
                                metavar='log_config_file',
                                default=logging_conf)
        else:
            parser.add_argument('--log-conf',
                                help='logger configuration file',
                                metavar='log_config_file',
                                default=DEFAULT_LOG_CONFIG_FILE)
        args, script = parser.parse_known_args(sys.argv)
        options = vars(args)
        logging.config.fileConfig(options['log_conf'])
        __ready_to_use__ = True
