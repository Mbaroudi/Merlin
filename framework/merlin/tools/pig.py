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
Pig client

Apache Pig is a platform for analyzing large data sets that consists of a high-level language
for expressing data analysis programs, coupled with infrastructure for evaluating these programs.
The salient property of Pig programs is that their structure is amenable to substantial
parallelization, which in turns enables them to handle very large data sets.

Pig's infrastructure layer consists of a compiler that produces sequences of Map-Reduce programs,
for which large-scale parallel implementations already exist (e.g., the Hadoop subproject).
Pig's language layer currently consists of a textual language called Pig Latin.

This client provides Python wrapper for Pig command-line interface

PIG JOB EXAMPLES :

Runs Pig script from file :
        Pig.load_commands_from_file("/tmp/file").run()

    Will be transformed to next Pig CLI command :
    pig -f /tmp/file

Runs Pig script from given string :
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").run()

    Will be transformed to next Pig CLI command :
    pig -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"


Runs Pig Job with parameters from file at the given path :
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").load_parameters_from_file('/tmp/file').run()

    Will be transformed to next Pig CLI command :
    pig -param_file /tmp/file -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"


Runs Pig Job with parameter :
        Pig.load_commands_from_file("/tmp/file").
        with_parameter("student", "'people'").run()

    Will be transformed to next Pig CLI command :
    pig -param student='people' -f "/tmp/file"


Runs Pig Job with pig's logger :
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").log_config(logfile="/tmp/log", debug=True,
                             warning=True, brief=True).run()

    Will be transformed to next Pig CLI command :
    pig -logfile /tmp/log -brief -warning -debug
    -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"


Runs Pig Job with property file at the given path :
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").with_property_file('/tmp/file').run()

    Will be transformed to next Pig CLI command :
    pig -propertyFile /tmp/file -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"

Runs Pig Job without optimization 'Split filter conditions':
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").without_split_filter().run()

    Will be transformed to next Pig CLI command :
    pig -optimizer_off SplitFilter -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"

    Also can runs Pig Job without optimizations using next method:
        .without_pushup_filter() PushUpFilter - Filter as early as possible
        .without_merge_filter() MergeFilter - Merge filter conditions
        .without_push_down_foreach_flatten() PushDownForeachFlatten
        - Join or explode as late as possible
        .without_limit_optimizer() LimitOptimizer - Limit as early as possible
        .without_column_map_key_prune() ColumnMapKeyPrune - Remove unused data
        .without_add_foreach() AddForEach - Add ForEach to remove unneeded columns
        .without_merge_foreach() MergeForEach - Merge adjacent ForEach
        .without_groupby_const_parallel_setter() GroupByConstParallelSetter
        - Force parallel 1 for "group all" statement

Runs Pig Job without multi query optimization :
        Pig.load_commands_from_string("A = LOAD 'student' USING PigStorage()
        AS (name:chararray, age:int, gpa:float);
        B = FOREACH A GENERATE name;
        DUMP B;").without_multiquery().run()

    Will be transformed to next Pig CLI command :
    pig -no_multiquery -e "A = LOAD 'student' USING PigStorage()
    AS (name:chararray, age:int, gpa:float);
    B = FOREACH A GENERATE name;
    DUMP B;"

Runs Pig Job using tez execution mode:
        Pig.load_commands_from_file(
            path='wordcount.pig').using_mode(type="tez").run()
    Will be transformed to next Pig CLI command :
    pig -x tez -f "wordcount.pig"


KNOWN ISSUES AND LIMITATIONS:
1. Parameter substitution does not work while trying to run command from string
     >> pig -param input_dir=/tmp/pigk8V7sk.txt \
        -param output_dir=/tmp/data_a75df3f2-1789-4ebf-8212-7cffa8667419 \
        -e "A = load '$input_dir' using PigStorage(',');\
        B = foreach A generate \$0 as id;\
        STORE B into '$output_dir';"

    will raise java.lang.IllegalArgumentException: Can not create a Path from an empty string

    >> pig -e "A = load '/tmp/pigh2xkXw.txt' using PigStorage(',');\
        B = foreach A generate \$0 as id;\
        STORE B into '/tmp/data_62158ce6-774d-4cf7-8d16-a305cdf05afb';"
    will produce next output:
        Input(s):
        Successfully read 1 records (382 bytes) from: "/tmp/pigh2xkXw.txt"

        Output(s):
        Successfully stored 1 records (6 bytes) in: "/tmp/data_62158ce6-774d-4cf7-8d16-a305cdf05afb"


    Described above behaviour was reproduced on:
     - Apache Pig version 0.12.0-cdh5.2.0

"""

import uuid
from merlin.common.logger import get_logger
from merlin.common.exceptions import PigCommandError
from merlin.common.configurations import Configuration
from merlin.common.shell_command_executor import execute_shell_command


class Pig(object):
    """ Wrapper for pig command line utility. Provides logic to configure and launch Pig scripts"""
    LOG = get_logger("Pig")

    @staticmethod
    def load_commands_from_file(path,
                                command_executor=execute_shell_command):
        """
        Creates an instance of Pig client.
        Configures Pig client to run commands from specified script file.
        :param path: path to the script to execute
        :param command_executor:  The interface used by the client to run command.

        :type path: str
        :rtype: Pig
        """
        Pig.LOG.info("Loading Pig script from file : {0}".format(path))
        _config = Configuration.create(readonly=False, accepts_nulls=True)
        _job_name = "PIG_TASK_{0}".format(uuid.uuid4())
        _pig = Pig(config=_config,
                   job_name=_job_name,
                   command_executor=command_executor)
        _pig.execute_script(path=path)
        return _pig

    @staticmethod
    def load_commands_from_string(commands,
                                  command_executor=execute_shell_command):
        """
         Creates an instance of Pig client.
         Configures Pig client to parse and run commands from string.
         :param commands: Commands to execute (within quotes)
         :param command_executor:  The interface used by the client to run command.

         :type commands: str
         :rtype: Pig
         """
        _config = Configuration.create(readonly=False, accepts_nulls=True)
        _job_name = "PIG_TASK_{0}".format(uuid.uuid4())
        _pig = Pig(config=_config,
                   job_name=_job_name,
                   command_executor=command_executor)
        _pig.execute_commands(commands=commands)
        return _pig

    @staticmethod
    def load_preconfigured_job(config, job_name, command_executor=execute_shell_command):
        """
        Creates a pre-configured instance of the Pig client.
        :param config: pig job configurations
        :param job_name: ig job identifier.
             Will be used as a name of the section with job-specific configurations.
        :param command_executor:
        :return:
        """
        return Pig(config=config,
                   job_name=job_name,
                   command_executor=command_executor)

    def __init__(self, config, job_name, command_executor):
        self._config = config
        self._job_name = job_name
        self._command_executor = command_executor

    def __add_config_option__(self, key, value):
        """Facade method used to add new options to job-specific section of the configuration"""
        self._config.set(section=self._job_name, key=key, value=value)

    def _has_config_option_(self, key):
        """Facade method used to check if option with specified name is exist in configuration"""
        return self._config.has(section=self._job_name, key=key)

    def _get_config_option_(self, key):
        """Facade method used to get option from configuration"""
        return self._config.get(
            section=self._job_name,
            key=key)

    def _wrap_with_quotes_(self, value):
        """Wraps string with quotes: single or double"""
        if not value or value[0] in ['"', "'"]:
            return value
        _template = "'{}'" if '"' in value else '"{}"'
        return _template.format(value)

    def execute_script(self, path):
        """
        Specifies file containing script to execute.
        :param path: Path to the script to execute
            Will be passed to command executor as a value of -file option
        :rtype: Pig
        """
        if path:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_SCRIPT_FILE,
                                       path)
        return self

    def execute_commands(self, commands):
        """
        Specifies commands to execute
        :param commands: Commands to execute (within quotes)
        :rtype: Pig
        """
        if commands:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_COMMANDS_STRING,
                                       commands)
            return self

    def _configure_command_(self):
        """Adds pig commands to cli call."""
        if self._has_config_option_(TaskOptions.CONFIG_KEY_SCRIPT_FILE):
            return ['-f', self._wrap_with_quotes_(
                self._get_config_option_(key=TaskOptions.CONFIG_KEY_SCRIPT_FILE)
            )]
        elif self._has_config_option_(TaskOptions.CONFIG_KEY_COMMANDS_STRING):
            return ['-e', self._wrap_with_quotes_(
                self._get_config_option_(key=TaskOptions.CONFIG_KEY_COMMANDS_STRING)
            )]
        else:
            raise PigCommandError(
                "Failed to configure command : one of {} or {} is required".format(
                    TaskOptions.CONFIG_KEY_SCRIPT_FILE,
                    TaskOptions.CONFIG_KEY_COMMANDS_STRING)
            )

    def _configure_pig_options_(self, verbose=False):
        """Parse job specific configurations and builds arguments to be passed to CLI call"""
        _options = []
        if verbose:
            _options.append('-verbose')
        _options.extend(self.__configure_logging__())
        self.__add_command_arg__("-param_file",
                                 TaskOptions.CONFIG_KEY_PARAMETER_FILE,
                                 _options)

        if self._has_config_option_(TaskOptions.CONFIG_KEY_PARAMETER_VALUE):
            _params = self._config.get_list(self._job_name,
                                            TaskOptions.CONFIG_KEY_PARAMETER_VALUE)
            if _params:
                _options.extend(["-param {}".format(param) for param in _params])

        self.__add_command_arg__("-propertyFile",
                                 TaskOptions.CONFIG_KEY_PROPERTIES_FILE,
                                 _options)
        if self._has_config_option_(TaskOptions.CONFIG_KEY_EXECTYPE):
            _options.append("-x {}".format(self._get_config_option_(TaskOptions.CONFIG_KEY_EXECTYPE)))
        _options.extend(self._disable_optimizations_())

        return _options

    def __configure_logging__(self):
        """add logging configurations to cli call"""
        _logging_options_ = []
        self.__add_command_arg__("-log4jconf",
                                 TaskOptions.CONFIG_KEY_LOG4J,
                                 _logging_options_)
        self.__add_command_arg__("-logfile",
                                 TaskOptions.CONFIG_KEY_LOG_FILE,
                                 _logging_options_)
        self.__add_command_marker_arg("-brief",
                                      TaskOptions.CONFIG_KEY_LOG_BRIEF,
                                      _logging_options_)
        self.__add_command_marker_arg("-warning",
                                      TaskOptions.CONFIG_KEY_LOG_WARNING,
                                      _logging_options_)
        self.__add_command_marker_arg("-debug",
                                      TaskOptions.CONFIG_KEY_LOG_DEBUG,
                                      _logging_options_)

        return _logging_options_

    def _disable_optimizations_(self):
        """add cli call args to disable Pig Job optimizations"""
        _optimizations = []
        _optimizations.extend(
            self._configure_optimization_rule(TaskOptions.CONFIG_KEY_DISABLE_SPLIT_FILTER)
        )
        _optimizations.extend(
            self._configure_optimization_rule(TaskOptions.CONFIG_KEY_DISABLE_PUSHUP_FILTER)
        )
        _optimizations.extend(
            self._configure_optimization_rule(TaskOptions.CONFIG_KEY_DISABLE_MERGE_FILTER)
        )
        _optimizations.extend(
            self._configure_optimization_rule(
                TaskOptions.CONFIG_KEY_DISABLE_PUSHDOWN_FOREACH_FLATTEN
            )
        )
        _optimizations.extend(
            self._configure_optimization_rule(TaskOptions.CONFIG_KEY_DISABLE_LIMIT_OPTIMIZER)
        )
        _optimizations.extend(
            self._configure_optimization_rule(
                TaskOptions.CONFIG_KEY_DISABLE_COLUMN_MAP_KEY_PRUNE
            )
        )
        _optimizations.extend(
            self._configure_optimization_rule(
                TaskOptions.CONFIG_KEY_DISABLE_ADD_FOREACH
            )
        )
        _optimizations.extend(
            self._configure_optimization_rule(
                TaskOptions.CONFIG_KEY_DISABLE_MERGE_FOREACH
            )
        )
        _optimizations.extend(
            self._configure_optimization_rule(
                TaskOptions.CONFIG_KEY_DISABLE_GROUPBY_CONST_PARALLEL_SETTER
            )
        )
        _optimizations.extend(
            self._configure_optimization_rule(TaskOptions.CONFIG_KEY_DISABLE_ALL)
        )
        if self.is_optimization_disabled(TaskOptions.CONFIG_KEY_DISABLE_MULTIQUERY):
            _optimizations.append('-no_multiquery')

        return _optimizations

    def _configure_optimization_rule(self, rule_name):
        """build cli parameter to disable specific optimization rule"""
        return ['-optimizer_off', rule_name] if self.is_optimization_disabled(rule_name) else []

    def __add_command_arg__(self, name, config_key, args=list()):
        """adds argument to cli call"""
        if self._has_config_option_(config_key):
            args.extend([name, self._get_config_option_(config_key)])

    def __add_command_marker_arg(self, name, config_key, args=list()):
        """adds marker argument (argument without value) to cli call"""
        if self._has_config_option_(config_key) and self._get_config_option_(config_key):
            args.append(name)

    def log_config(self,
                   logfile=None,
                   debug=False,
                   warning=False,
                   brief=False):
        """
        Adds and configures custom logger for Pig's script

        :param logfile: to file, that will have logs from Pig Job
        :param debug: Enables debug level. Default it is False
        :param warning: Enables warning level. Default it is False
            Also turns warning aggregation off
        :param brief: Enables Brief logging (no timestamps). Default it is False

        :type logfile: str
        :type debug: bool
        :type warning bool
        :type brief: bool
        :rtype: Pig
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_LOG_FILE, logfile)
        if debug:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_LOG_DEBUG, "enabled")
        if warning:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_LOG_WARNING, "enabled")
        if brief:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_LOG_BRIEF, "enabled")
        return self

    def log4j_config(self, path):
        """
        Specify Log4j configuration file, overrides log conf
        :param path: path to file with log4j parameters for Pig Job
        :return:
        """
        if path:
            self.__add_config_option__(TaskOptions.CONFIG_KEY_LOG4J, path)
        return self

    def with_parameter(self, key, value):
        """
        Sets parameter for Pig Job script in the next format:
        name=value
        :param key: key to parameter
        :param value: value of parameter
        :type key: str
        :type value: str
        :rtype: Pig
        """
        self._config.update_list(self._job_name,
                                 TaskOptions.CONFIG_KEY_PARAMETER_VALUE,
                                 "{}={}".format(key, value))
        return self

    def load_parameters_from_file(self, path):
        """
        Specifies file with parameters
        :param path: Path to the parameter file
        :return:
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_PARAMETER_FILE, path)
        return self

    def with_property_file(self, path):
        """
        Sets file with properties at the given path
        :param path: to file with properties for Pig Job
        :type path: str
        :rtype: Pig
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_PROPERTIES_FILE, path)
        return self

    def without_split_filter(self):
        """
        Job will run without optimization 'Split filter conditions'

        Optimization split filter condition to allow push filter more aggressively.
        e.g.:
            D = FILTER C BY a1>0 and b1>0;
        will be splitted into:
            X = FILTER C BY a1>0;
            D = FILTER X BY b1>0;
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_SPLIT_FILTER, 'disable')
        return self

    def without_pushup_filter(self):
        """
        Job will run without optimization 'Early Filters'
        The objective of this optimization rule is to push
        the FILTER operators up the data flow graph.
        As a result, the number of records that flow through the pipeline is reduced.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_PUSHUP_FILTER, 'disable')
        return self

    def without_merge_filter(self):
        """
        Job will run without optimization 'Merge filter conditions'
        This rule used to merge filter conditions after PushUpFilter
        rule to decrease the number of filter statements.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_MERGE_FILTER, 'disable')
        return self

    def without_push_down_foreach_flatten(self):
        """
        Job will run without optimization 'Join or explode as late as possible'
        The objective of this rule is to reduce the number of records that flow
        through the pipeline by moving FOREACH operators with a FLATTEN down the data flow graph.
        """
        self.__add_config_option__(
            TaskOptions.CONFIG_KEY_DISABLE_PUSHDOWN_FOREACH_FLATTEN,
            'disable'
        )
        return self

    def without_limit_optimizer(self):
        """
        Job will run without optimization 'Limit as early as possible'
        The objective of this rule is to push the LIMIT operator up the data flow graph.
        In addition, for top-k (ORDER BY followed by a LIMIT) the LIMIT
        is pushed into the ORDER BY.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_LIMIT_OPTIMIZER, 'disable')
        return self

    def without_column_map_key_prune(self):
        """
        Job will run without optimization 'Remove unused data'
        Prune the loader to only load necessary columns.
        The performance gain is more significant if the corresponding loader support column pruning
        and only load necessary columns.
        Otherwise, ColumnMapKeyPrune will insert a ForEach statement right after loader.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_COLUMN_MAP_KEY_PRUNE,
                                   'disable')
        return self

    def without_add_foreach(self):
        """
        Job will run without optimization 'Add ForEach to remove unneeded columns'
        Prune unused column as soon as possible.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_ADD_FOREACH,
                                   'disable')
        return self

    def without_merge_foreach(self):
        """
        Job will run without optimization 'Merge adjacent ForEach'
        The objective of this rule is to merge together two foreach statements,
        if these preconditions are met:

            - The foreach statements are consecutive.
            - The first foreach statement does not contain flatten.
            - The second foreach is not nested.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_MERGE_FOREACH,
                                   'disable')
        return self

    def without_groupby_const_parallel_setter(self):
        """
        Job will run without optimization 'Force parallel 1 for "group all" statement'
        Force parallel "1" for "group all" statement.
        That's because even if we set parallel to N, only 1 reducer
        will be used in this case and all other reducer produce empty result.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_GROUPBY_CONST_PARALLEL_SETTER,
                                   'disable')
        return self

    def without_multiquery(self):
        """
       Turns off multi query optimization.
       Default multi query optimization is turned on
       :rtype: Pig
       """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_MULTIQUERY,
                                   'disable')
        return self

    def disable_all_optimizations(self):
        """Disables all optimizations"""
        self.__add_config_option__(TaskOptions.CONFIG_KEY_DISABLE_ALL,
                                   'disable')
        return self

    def run(self, debug=False):
        """
        Runs Pig Job
        :rtype: Result
        """
        Pig.LOG.info("Running Pig Job")
        command_args = self._configure_pig_options_(debug) + self._configure_command_()
        return self._command_executor('pig', *command_args)

    def debug(self):
        """Runs Pig script in debug mode."""
        return self.run(debug=True)

    def is_optimization_disabled(self, optimization_config_key, disable_marker='disable'):
        """
        Checks is specified optimization is disabled.
        By default optimization, and all optimization rules, are turned on.
        :param optimization_config_key:
        :param disable_marker:
        :return:
        """
        return self._has_config_option_(optimization_config_key) \
               and disable_marker == self._get_config_option_(optimization_config_key)

    def using_mode(self, type="mapreduce"):
        """
        Sets execution mode, default is mapreduce.
        """
        self.__add_config_option__(TaskOptions.CONFIG_KEY_EXECTYPE,
                                   type)
        return self


class TaskOptions(object):
    """
    The following options can be used to configure Pig job
    """
    # Path to the script to execute
    CONFIG_KEY_SCRIPT_FILE = 'file'
    # Commands to execute (within quotes)
    CONFIG_KEY_COMMANDS_STRING = 'commands'
    # Log4j configuration file, overrides log conf
    CONFIG_KEY_LOG4J = "log4jconf"
    # Set execution mode: local|mapreduce, default is mapreduce.
    CONFIG_KEY_EXECTYPE = "exectype"
    # Path to client side log file; default is current working directory.
    CONFIG_KEY_LOG_FILE = "logfile"
    # Brief logging (no timestamps)
    CONFIG_KEY_LOG_BRIEF = "brief"
    # Debug level, INFO is default
    CONFIG_KEY_LOG_DEBUG = "debug"
    # Turn warning logging on; also turns warning aggregation off
    CONFIG_KEY_LOG_WARNING = "warning"
    # Key value pair of the form param=val
    CONFIG_KEY_PARAMETER_VALUE = "param"
    # Path to the parameter file
    CONFIG_KEY_PARAMETER_FILE = "param_file"
    # Path to property file
    CONFIG_KEY_PROPERTIES_FILE = "propertyFile"
    # Turn optimizations off. The following values are supported:
    CONFIG_KEY_DISABLE_SPLIT_FILTER = "SplitFilter"
    CONFIG_KEY_DISABLE_PUSHUP_FILTER = "PushUpFilter"
    CONFIG_KEY_DISABLE_MERGE_FILTER = "MergeFilter"
    CONFIG_KEY_DISABLE_PUSHDOWN_FOREACH_FLATTEN = "PushDownForeachFlatten"
    CONFIG_KEY_DISABLE_LIMIT_OPTIMIZER = "LimitOptimizer"
    CONFIG_KEY_DISABLE_COLUMN_MAP_KEY_PRUNE = "ColumnMapKeyPrune"
    CONFIG_KEY_DISABLE_ADD_FOREACH = "AddForEach"
    CONFIG_KEY_DISABLE_MERGE_FOREACH = "MergeForEach"
    CONFIG_KEY_DISABLE_GROUPBY_CONST_PARALLEL_SETTER = "GroupByConstParallelSetter"
    # Disable all optimizations
    CONFIG_KEY_DISABLE_ALL = "All"
    # Turn multiquery optimization off; default is on
    CONFIG_KEY_DISABLE_MULTIQUERY = "no_multiquery"
