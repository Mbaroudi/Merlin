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
Flume client.

Apache Flume is a distributed, reliable, and available system for efficiently collecting,
aggregating and moving large amounts of log data from many different sources to a centralized data store.

The use of Apache Flume is not only restricted to log data aggregation. Since data sources are customizable,
Flume can be used to transport massive quantities of event data including but not limited to network traffic data,
social-media-generated data, email messages and pretty much any data source possible

This client provides Python wrapper for Flume command-line interface

FLUME AGENT EXAMPLES :

Runs Flume Agent 'a1' with configuration file at the given path 'path/to/file':

    Flume.agent(agent="a1", conf_file="path/to/file").run()

Will be transformed to next Flume CLI command :

    flume-ng agent --name a1 --conf-file path/to/file


Runs Flume Agent with configuration directory:

    Flume.agent(agent="a1", conf_file="path/to/file")\
            .using_conf_dir("path/to/dir").run()

Will be transformed to next Flume CLI command :

    flume-ng agent --name a1 --conf-file path/to/file --conf path/to/dir


Runs Flume Agent with additional third-plugins directories:

    Flume.agent(agent="a1", conf_file="path/to/file")\
            .using_plugins_directories(pathes=["path/to/plugins1,path/to/plugins2", "path/to/plugins3"])\
            .run()

Will be transformed to next Flume CLI command :

    flume-ng agent --name a1 --conf-file path/to/file \
    --plugins-path path/to/plugins1,path/to/plugins2,path/to/plugins3


Runs Flume Agent with additional properties and options:

    Flume.agent(agent="a1", conf_file="path/to/file")\
            .with_jvm_option(name="jvm_option", value="test")\
            .with_jvm_property(name="jvm_property", value="test")\
            .run()

Will be transformed to next Flume CLI command :

    flume-ng agent --name a1 --conf-file path/to/file \
    -Djvm_property=test -Xjvm_option=test
"""

import uuid
from merlin.common.configurations import Configuration
from merlin.common.exceptions import ConfigurationError

from merlin.common.logger import get_logger
from merlin.common.shell_command_executor import execute_shell_command
from merlin.common.utils import ListUtility


class Flume(object):
    """
    Wrapper for Flume command line utility
    """

    LOG = get_logger("Flume")

    @staticmethod
    def agent(agent=None, conf_file=None, executor=execute_shell_command):
        """
        Creates wrapper for 'flume-ng agent' command line commands
        :param agent: name of the agent
        :type agent: str
        :param conf_file: path to config file
        :type conf_file: str
        :rtype: FlumeAgent
        """
        return FlumeAgent(agent=agent, conf_file=conf_file, executor=executor)


class FlumeAgent(object):
    """
    Wrapper for Flume command line utility
    """
    def __init__(self, agent=None, conf_file=None, config=None,
                 executor=execute_shell_command):
        """
        Creates wrapper for Flume command line utility
        :param executor: custom executor
        :type executor:
        """

        self.name = agent if agent else "FLUME_AGENT_{0}".format(uuid.uuid4())
        self._executor = executor
        self._config = config if config else Configuration.create(
            readonly=False,
            accepts_nulls=True
        )
        self.__set_attr__(TaskOptions.CONFIG_KEY_AGENT_NAME, agent)
        self.__set_attr__(TaskOptions.CONFIG_KEY_CONF_FILE, conf_file)

    def __set_attr__(self, key=None, value=None):
        """
        Configuration method for set attribute
        :type key: str

        """
        if value:
            self._config.set(self.name, key, value)

    def __set_config(self, key, name, value):
        """
        Configuration method for add parameter
        :type key:
        :type name:
        :type value:
        """
        if name:
            if self.has_option(key):
                value = "{0}\n{1}={2}".format(self._config.
                                              get(self.name, key),
                                              name, value)
            else:
                value = "{0}={1}".format(name, value)

        self._config.set(self.name, key, value)

    def __get(self, key, required=False):
        """
        Get value by key from configuration
        """
        # try to load config option from job speciofic section
        _value = self._config.get(self.name, key)

        if not _value and required:
            raise ConfigurationError("Configuration option '{0}' is required".format(key))
        else:
            return _value

    def __build(self):
        """
        Build Flume's commmand
        :return: list of query params
        :rtype: list
        """

        params = ["agent", "--name {0}".format(self.__get(TaskOptions.CONFIG_KEY_AGENT_NAME,
                                                          required=True)),
                  "--conf-file {0}".format(self.__get(TaskOptions.CONFIG_KEY_CONF_FILE,
                                                      required=True))]

        if self.has_option(TaskOptions.CONFIG_KEY_CONF_DIR):
            params.append("--conf {0}".format(self.__get(TaskOptions.CONFIG_KEY_CONF_DIR)))
        if self.has_option(TaskOptions.CONFIG_KEY_PLUGINS_PATH):
            params.append("--plugins-path {0}".format(self.__get(TaskOptions.CONFIG_KEY_PLUGINS_PATH)))
        if self.has_option(TaskOptions.CONFIG_KEY_D_OPTIONS):
            list_ = self._config.get_list(self.name, TaskOptions.CONFIG_KEY_D_OPTIONS)
            for value in list_:
                params.append("-D{0}".format(value))
        if self.has_option(TaskOptions.CONFIG_KEY_X_OPTIONS):
            list_ = self._config.get_list(self.name, TaskOptions.CONFIG_KEY_X_OPTIONS)
            for value in list_:
                params.append("-X{0}".format(value))

        return " ".join(params)

    def run(self):
        """
        Run Flume's agent in cmd
        :rtype:
        """
        Flume.LOG.info("Running Flume Agent")
        result = self._executor("flume-ng", self.__build())

        return result

    def has_option(self, key):
        """
        Checks if attribute at the given key exists in job specific section of the Configuration.
        :param key: attribute name
        :return:  True in case attribute was found otherwise False
        """
        return self._config.has(section=self.name, key=key)

    def with_jvm_X_option(self, name, value):
        """
        Add java's property to cmd
        :param name:
        :param value:
        :return:
        """
        self.__set_config(TaskOptions.CONFIG_KEY_X_OPTIONS, name=name, value=value)
        return self

    def with_jvm_D_option(self, name, value):
        """
        Add java's option to cmd
        :param name:
        :param value:
        :return:
        """
        self.__set_config(TaskOptions.CONFIG_KEY_D_OPTIONS, name=name, value=value)
        return self

    def load_configs_from_dir(self, path):
        """
        Add configuration directory to cmd
        :param path:
        :param path:
        :return:
        """

        self.__set_attr__(TaskOptions.CONFIG_KEY_CONF_DIR, value=path)
        return self

    def load_plugins_from_dirs(self, pathes):
        """
        Add plugins directories to cmd
        :return:
        """
        if isinstance(pathes, list):
            pathes = ListUtility.to_string(pathes)
        self.__set_attr__(TaskOptions.CONFIG_KEY_PLUGINS_PATH, value=pathes)
        return self


class TaskOptions(Flume):
    """
    Option names used to configure Flume agent
    """
    #the name of this agent
    CONFIG_KEY_AGENT_NAME = 'agent.name'
    #specify a config file
    CONFIG_KEY_CONF_FILE = 'conf.file'

    #sets a Java system property values
    CONFIG_KEY_D_OPTIONS = "Doptions"
    #sets a Java -X options
    CONFIG_KEY_X_OPTIONS = "Xoptions"
    #use configs in <conf> directory
    CONFIG_KEY_CONF_DIR = 'conf.dir'
    #colon-separated list of plugins.d directories
    CONFIG_KEY_PLUGINS_PATH = 'plugins.path'


