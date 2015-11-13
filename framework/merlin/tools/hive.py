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
Hive client.

The Apache Hive data warehouse software facilitates querying and managing large
datasets residing in distributed storage. Hive provides a mechanism to project structure
onto this data and query the data using a SQL-like language called HiveQL.
At the same time this language also allows traditional map/reduce programmers
to plug in their custom mappers and reducers when it is inconvenient
or inefficient to express this logic in HiveQL.

This client provides Python wrapper for Hive command-line interface

HIVE JOB EXAMPLES :

Runs Hive query from file :
        Hive.load_queries_from_file("/tmp/file").run()

    Will be transformed to next Hive CLI command :
    hive -f /tmp/file

Runs Hive query from given string :
        Hive.load_queries_from_string('CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);').run()

    Will be transformed to next Hive CLI command :
    hive -e "CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);"

Runs Hive Job with configuration :
        Hive.load_queries_from_string("CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);").with_hive_conf('name', 'value').run()

    Will be transformed to next Hive CLI command :
    hive -e 'CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);' --hiveconf name=value

Runs Hive Job using the database 'db_name':
        Hive.load_queries_from_string("CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);").use_database('db_name').run()

    Will be transformed to next Hive CLI command :
    hive -e "CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);" --database db_name

Runs Hive Job with auxpath :
        Hive.load_queries_from_string("CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);").with_auxillary_jars('/tmp/.jar').run()

    Will be transformed to next Hive CLI command :
    hive -e "CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);" --auxpath /tmp/.jar

Runs Hive Job with defines variable :
        Hive.load_queries_from_string("CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);").define_variable('name', 'value').run()

    Will be transformed to next Hive CLI command :
    hive -e "CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);" --define name=value

Runs Hive Job with adds hivevar :
        Hive.load_queries_from_string("CREATE TABLE invites (foo INT, bar STRING)
        PARTITIONED BY (ds STRING);").add_hivevar('name', 'value').run()

    Will be transformed to next Hive CLI command :
    hive -e "CREATE TABLE invites (foo INT, bar STRING)
    PARTITIONED BY (ds STRING);" --hivevar name=value



--------------------------
ISSUE
--------------------------

hive -e "select * from default.page_view where userid = '$s'" --define s=2 --database default
!!!does not replace for 'userid = 2'

hive -f "....path...." --define s=2 --database default  (file contains next query :
"select * from default.page_view where userid = '$s'")
!!!replace

"""
from merlin.common.configurations import Configuration

import uuid

from merlin.common.logger import get_logger
from merlin.common.exceptions import HiveCommandError
from merlin.common.shell_command_executor import execute_shell_command


class Hive(object):
    """
    Wrapper for Hive command line utility
    """

    LOG = get_logger('Hive')

    @staticmethod
    def load_queries_from_file(path, executor=execute_shell_command):
        """
        Creates wrapper for hive command line utility with execute query from file
        :param path: to file with query for Hive Job
        :type path: str
        :rtype: Hive
        """

        Hive.LOG.info("Loading Hive queries from file : {0}".format(path))
        hive = Hive(executor=executor)
        hive.execute_script(path)
        return hive

    @staticmethod
    def load_queries_from_string(query, executor=execute_shell_command):
        """
        Creates wrapper for hive command line utility with execute query from string
        :param query: HiveQL's query for executing
        :type query: str
        :rtype: Hive
        """

        Hive.LOG.info("Loading Hive queries from string : {0}".format(query))
        hive = Hive(executor=executor)
        hive.execute_commands(query)
        return hive

    @staticmethod
    def load_preconfigured_job(name=None, config=None, executor=execute_shell_command):
        """
        Creates wrapper for hive command line utility. Configure it with options
        :param config: hive job configurations
        :param name: hive job identifier.
             Will be used as a name of the section with job-specific configurations.
        :param executor:
        :return:
        """
        Hive.LOG.info("Loading Hive queries from configuration")

        return Hive(name=name, config=config, executor=executor)

    def __init__(self, name=None, config=None, executor=execute_shell_command):
        """
        Creates wrapper for Hive command line utility
        :param executor: custom executor
        :type executor:
        """

        super(Hive, self).__init__()
        self.name = name if name else "HIVE_TASK_{0}".format(uuid.uuid4())
        self.__executor = executor
        self._config = config if config else Configuration.create(
            readonly=False,
            accepts_nulls=True
        )

    def _wrap_with_quotes_(self, value):
        if not value or value[0] in ['"', "'"]:
            return value
        return "\"{0}\"".format(value)

    def execute_script(self, path):
        """
        Specifies file containing script to execute.
        :param path: Path to the script to execute
            Will be passed to command executor as a value of -f option
        :rtype: Hive
        """
        if path:
            self.__set_config(TaskOptions.CONFIG_KEY_QUERY_FILE, None,
                              path)
        return self

    def execute_commands(self, commands):
        """
        Specifies commands to execute
        :param commands: Commands to execute (within quotes)
        :rtype: Hive
        """
        if commands:
            self.__set_config(TaskOptions.CONFIG_KEY_COMMANDS_STRING, None,
                              commands)
            return self

    def _configure_command_(self):
        if self.has_option(TaskOptions.CONFIG_KEY_QUERY_FILE):
            return ['-f', self._wrap_with_quotes_(self._config.get(section=self.name,
                                                  key=TaskOptions.CONFIG_KEY_QUERY_FILE))]
        elif self.has_option(TaskOptions.CONFIG_KEY_COMMANDS_STRING):
            return ['-e', self._wrap_with_quotes_(self._config.get(section=self.name,
                                                  key=TaskOptions.CONFIG_KEY_COMMANDS_STRING))]
        else:
            raise HiveCommandError("Failed to configure command : one of {0} or {0} is required".format(
                TaskOptions.CONFIG_KEY_QUERY_FILE,
                TaskOptions.CONFIG_KEY_COMMANDS_STRING))

    def with_hive_conf(self, name, value):
        """
        Adds hive's configuration to Hive Job
        :param name: name of the given configuration
        :param value: value of the given configuration
        :type name: str
        :type value: str
        :rtype:
        """

        key = TaskOptions.CONF_KEY_HIVE_CONFIG
        self.__set_config(key, name, value)
        return self

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

    def add_hivevar(self, name, value):
        """
        Sets hive's variable to job's context
        :param name: name of the given variable
        :param value: value of the given variable
        :type name: str
        :type value: str
        :rtype: Hive
        """
        key = TaskOptions.CONF_KEY_HIVE_VAR
        self.__set_config(key, name, value)
        return self

    def define_variable(self, name, value):
        """
        Sets hive's variable to job's context
        :param name: name of the given variable
        :param value: value of the given variable
        :type name: str
        :type value: str
        :rtype: Hive
        """
        key = TaskOptions.CONF_KEY_DEFINE
        self.__set_config(key, name, value)
        return self

    def use_database(self, database):
        """
        Sets database to job's context
        :param database: name of the custom database
        :type database: str
        :rtype: Hive
        """
        key = TaskOptions.CONF_KEY_DATABASE
        self.__set_config(key, None, database)
        return self

    def with_auxillary_jars(self, jars):
        """
        Sets the path to jar that contain implementations of user defined functions and serdes
        :param jars: paths to jar
        :type jars: list, str
        :rtype: Hive
        """
        if isinstance(jars, list):
            jars = ",".join(jars)
        key = TaskOptions.CONF_KEY_AUXPATH
        self.__set_config(key, None, jars)
        return self

    def run(self):
        """
        Runs Hive Job
        :rtype:
        """
        Hive.LOG.info("Executing Hive Job")
        result = self.__executor("hive", self.build())
        result.if_failed_raise(HiveCommandError("Hive Job failed"))
        return result

    def build(self):
        """
        Builds query params for hive's query
        :return: list of query params
        :rtype: list
        """
        params = []
        if self.has_option(TaskOptions.CONF_KEY_AUXPATH):
            params.append("--auxpath {0}".format(self._config.get(self.name,
                                                                  TaskOptions.CONF_KEY_AUXPATH)))
        params.extend(self._configure_command_())
        if self.has_option(TaskOptions.CONF_KEY_DEFINE):
            list_ = self._config.get_list(self.name, TaskOptions.CONF_KEY_DEFINE)
            for value in list_:
                params.append("--define")
                params.append(value)
        if self.has_option(TaskOptions.CONF_KEY_HIVE_CONFIG):
            list_ = self._config.get_list(self.name, TaskOptions.CONF_KEY_HIVE_CONFIG)
            for value in list_:
                params.append("--hiveconf")
                params.append(value)
        if self.has_option(TaskOptions.CONF_KEY_HIVE_VAR):
            list_ = self._config.get_list(self.name, TaskOptions.CONF_KEY_HIVE_VAR)
            for value in list_:
                params.append("--hivevar")
                params.append(value)
        if self.has_option(TaskOptions.CONF_KEY_DATABASE):
            params.append("--database {0}".format(self._config.get(self.name,
                                                                   TaskOptions.CONF_KEY_DATABASE)))
        return " ".join(params)

    def has_option(self, key):
        """
        Checks if attribute at the given key exists in job specific section of the Configuration.
        :param key: attribute name
        :return:  True in case attribute was found otherwise False
        """
        return self._config.has(section=self.name, key=key)


class TaskOptions(Hive):
    """
    Option names used to configure Hive Job
    """
    #SQL from files
    CONFIG_KEY_QUERY_FILE = 'hive.file'
    #SQL from string
    CONFIG_KEY_COMMANDS_STRING = 'hive.commands'
    #variable subsitution to apply to hive
    #commands. e.g. -d A=B or --define A=B
    CONF_KEY_DEFINE = "define"
    #use value for given property <property=value>
    CONF_KEY_HIVE_CONFIG = "hiveconf"
    #variable subsitution to apply to hive
    #commands. e.g. --hivevar A=B
    CONF_KEY_HIVE_VAR = "hivevar"
    #specify the database to use
    CONF_KEY_DATABASE = "database"
    #the location of the plugin jars that contain implementations
    # of user defined functions and serdes
    CONF_KEY_AUXPATH = "auxpath"

