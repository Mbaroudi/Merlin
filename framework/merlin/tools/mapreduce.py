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
MapReduce client.

Hadoop MapReduce is a software framework for easily writing applications which process
vast amounts of data (multi-terabyte data-sets) in-parallel on large clusters
(thousands of nodes) of commodity hardware in a reliable, fault-tolerant manner.

A MapReduce job usually splits the input data-set into independent chunks which are processed
by the map tasks in a completely parallel manner. The framework sorts the outputs of the maps,
which are then input to the reduce tasks. Typically both the input and the output of the job
are stored in a file-system. The framework takes care of scheduling tasks, monitoring them
and re-executes the failed tasks.

Typically the compute nodes and the storage nodes are the same, that is,
the MapReduce framework and the Hadoop Distributed File System (see HDFS Architecture Guide)
are running on the same set of nodes. This configuration allows the framework
to effectively schedule tasks on the nodes where data is already present,
resulting in very high aggregate bandwidth across the cluster.

The MapReduce framework consists of a single master JobTracker and one slave
TaskTracker per cluster-node.
The master is responsible for scheduling the jobs' component tasks on the slaves, monitoring them
and re-executing the failed tasks. The slaves execute the tasks as directed by the master.

Minimally, applications specify the input/output locations and supply map and reduce functions
via implementations of appropriate interfaces and/or abstract-classes. These,
and other job parameters, comprise the job configuration. The Hadoop job client then
submits the job (jar/executable etc.) and configuration to the JobTracker which then
assumes the responsibility of distributing the software/configuration to the slaves,
scheduling tasks and monitoring them, providing status and
diagnostic information to the job-client.

This client provides Python wrapper for MapReduce command-line interface

MAPREDUCE JOB EXAMPLES :

MapReduce Job configured via HF API

        MapReduce.prepare_mapreduce_job(
             jar="hadoop-mapreduce-examples.jar",
             main_class="wordcount",
             name=_job_name
         ).with_config_option("split.by", "'\\t'") \
             .with_number_of_reducers(3) \
             .with_arguments("/user/vagrant/dmode.txt", "/tmp/test") \
             .run()
    Will be transformed to next MapReduce CLI command :

        hadoop jar hadoop-mapreduce-examples.jar wordcount -D mapreduce.job.name=%s -D split.by='\\t' -D mapreduce.job.reduces=3 /user/vagrant/dmode.txt /tmp/test

Pre-Configured MapReduce Job

        MapReduce.prepare_mapreduce_job(
                    jar='mr.jar',
                    main_class="test.mr.Driver",
                    config=Configuration.load(config_file=path_to_config_file),
                    name='simple_mr_job'
                ).run()
    Job Configurations

        [simple_mr_job]
        job.config = value.delimiter.char=,
            partition.to.process=20142010
        args=/input/dir
            /output/dir

    Will be transformed to next MapReduce CLI command :

        hadoop jar mr.jar test.mr.Driver -D mapreduce.job.name=simple_mr_job -D value.delimiter.char=, \
         -D partition.to.process=20142010 /input/dir /output/dir
        Streaming MapReduce Job

Streaming MapReduce Job configured via HF API

        MapReduce.prepare_streaming_job(
            name='job_name'
        ).take(
            'data'
        ).process_with(
            mapper='mapper.py',
            reducer='reducer.py'
        ).save(
            'output.txt'
        ).with_config_option(
            key='value.delimiter.char',
            value=','
        ).with_config_option(
            key='partition.to.process',
            value='20142010'
        ).run()
    Will be transformed to next MapReduce CLI command :

        hadoop jar hadoop-streaming.jar -D mapreduce.job.name=job_name \
        -D value.delimiter.char=, -D partition.to.process=20142010 \
        -mapper mapper.py -reducer reducer.py -input data -output output.txt

Pre-Configured Streaming MapReduce Job

        MapReduce.prepare_streaming_job(
                config=Configuration.load(config_file=path_to_config_file),
                name='streaming_test_job_with_multiple_inputs'
            ).run()
    Job Configurations

        [streaming_test_job_with_multiple_inputs]
        mapper= smapper.py
        reducer=sreducer.py
        input=/raw/20102014
            /raw/21102014
            /raw/22102014
        output=/core/20102014
        mapreduce.job.reduces=100
        inputformat='org.mr.CustomInputFormat'
        outputformat='org.mr.CustomOutputFormat'
        libjars=mr_001.jar
            mr_002.jar
        files=dim1.txt
        environment.vars=JAVA_HOME=/java
            tmp.dir=/tmp/streaming_test_job_with_multiple_inputs
    Will be transformed to next MapReduce CLI command :

        hadoop jar hadoop-streaming.jar -D mapreduce.job.name=streaming_test_job_with_multiple_inputs -files dim1.txt \
        -libjars mr_001.jar,mr_002.jar -mapper smapper.py -reducer sreducer.py -numReduceTasks 100 -input /raw/20102014 \
        -input /raw/21102014 -input /raw/22102014 -output /core/20102014 -inputformat org.mr.CustomInputFormat \
        -outputformat org.mr.CustomOutputFormat -cmdenv JAVA_HOME=/java -cmdenv tmp.dir=/tmp/streaming_test_job_with_multiple_inputs


--------------------------
ISSUE
--------------------------

"""


import re
import os
import uuid
from merlin.common.logger import get_logger
from merlin.common.configurations import Configuration
from merlin.common.shell_command_executor import execute_shell_command
from merlin.common.exceptions import CommandException, MapReduceConfigurationError


class MapReduce(object):
    LOG = get_logger("MapReduce")

    def __init__(self,
                 name,
                 config,
                 executable,
                 executor,
                 main_class=None,
                 shell_command="hadoop jar"):
        self.executor = executor
        self.executable = executable
        self._config = config if config else Configuration.create(
            readonly=False,
            accepts_nulls=True
        )
        self.name = name if name else "MR_TASK_{0}".format(uuid.uuid4())
        self.main_class = main_class
        self._shell_command = shell_command
        self._process = None

    @staticmethod
    def prepare_streaming_job(config=None,
                              name=None,
                              jar="hadoop-streaming.jar",
                              executor=execute_shell_command):
        """
        Creates instance of StreamingJob
        :param name: name of job
        :param jar: executing jar
        :param executor: interface used by the client to run command.
        :return: StreamingJob template
        :rtype : StreamingJob
        """
        MapReduce.LOG.info("MapReduce streaming job")
        config = config if config else Configuration.create(readonly=False, accepts_nulls=True)
        MapReduce.__validate_configs(config, name,
                                     "StreamingJob",
                                     TaskOptions.KEYS_FOR_MAPREDUCE)
        return StreamingJob(
            config=config,
            name=name if name else "MR_STREAMING_JOB_{0}".format(uuid.uuid4()),
            jar=jar,
            executor=executor
        )

    @staticmethod
    def prepare_mapreduce_job(jar,
                              main_class=None,
                              config=None,
                              name=None,
                              executor=execute_shell_command):
        """
        Creates instance of MapReduceJob
        :param name: name of job
        :param jar: executing jar
        :param executor: interface used by the client to run command.
        :return: MapReduceJob template
        :rtype : MapReduceJob
        """
        MapReduce.LOG.info("MapReduce job")
        config = config if config else Configuration.create(readonly=False, accepts_nulls=True)
        MapReduce.__validate_configs(config, name,
                                     "MapReduceJob",
                                     TaskOptions.KEYS_FOR_STREAMING_JOB)
        return MapReduceJob(
            name=name if name else "MR_JOB_{0}".format(uuid.uuid4()),
            config=config,
            jar=jar,
            main_class=main_class,
            executor=executor
        )

    @staticmethod
    def __validate_configs(config, name, type_of_job, keys):
        """
        Logs warning for set incorrect keys in .INI file for custom job
        """
        for key in keys:
            if config.has(name, key):
                MapReduce.LOG.warning("{0} does not use this key: {1}.".format(type_of_job, key))

    def run(self, *args):
        """
        Runs specific MapReduce Job
        :param args: specific argument to CLI for MapReduceJob
        :rtype:
        """
        if args:
            if isinstance(self, StreamingJob):
                MapReduce.LOG.warning("StreamingJob does not use args.")
            else:
                self._update_list_config_(TaskOptions.COMMAND_ARGS, *args)
        command, arguments = self.__configure_command__()
        self._process = self.executor(command, *arguments)
        return self._process

    def status(self):
        """
        Returns status of finished job
        :return:
        """
        return None if self._process.is_running() \
            else JobStatus(JobStatus.job_id(self._process.stderr))

    def with_number_of_reducers(self, reducer_num):
        """
        Streaming MR job has it's own command parameter to set number of reducers.
        Overrides base method to ignore 'mapreduce.job.reduces' configuration option
        :param reducer_num:
        :return:
        """
        return self.with_config_option(TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_NUM, reducer_num)

    def disable_reducers(self):
        return self.with_number_of_reducers(0)

    def __configure_command__(self):
        """Overrides this method to configure MR job"""
        if not os.path.isfile(self.executable):
            raise MapReduceConfigurationError("{0} doesn't exist".format(self.executable))
        arguments = [self.executable]
        if self.main_class is not None:
            arguments.append(self.main_class)
        arguments.extend(self._generic_options_())
        arguments.extend(self._command_options_())
        return self._shell_command, arguments

    def load_configuration_from(self, _file):
        """
        Specifies an application configuration file.
        :param _file:
        """
        self._config[TaskOptions.CONFIG_KEY_MR_JOB_CONF_FILE] = _file
        return self

    def use_jobtracker(self, jobtracker):
        """
        Specifies an application jobtracker.
        :param jobtracker:
        """
        self._config[TaskOptions.CONFIG_KEY_MR_JOBTRACKER] = jobtracker
        return self

    def _generic_options_(self):
        """
        Adds generic option to hadoop command.
        -conf <configuration file>
        -D <property>=<value>
        -jt <local> or <jobtracker:port> Specify a job tracker.
        -files <comma separated list of files> Specify comma separated files to be copied
        to the map reduce cluster.
        -libjars <comma separated list of jars> Specify comma separated jar files
        to include in the classpath.
        -archives <comma separated list of archives> Specify comma separated archives
        to be unarchived on the compute machines.

        Applications should implement Tool to support GenericOptions.

        :return:
        """
        options = []
        # Specify an application configuration file
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_CONF_FILE):
            options.extend(['--conf', self.get(TaskOptions.CONFIG_KEY_MR_JOB_CONF_FILE)])

        # Add or override MR job options
        options.extend(['-D', '='.join([TaskOptions.CONFIG_KEY_MR_JOB_NAME, self.name])])
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_CONF_OPTION):
            options.extend(
                "-D {0}".format(att) for att in self.get_list(
                    TaskOptions.CONFIG_KEY_MR_JOB_CONF_OPTION))

        # Specify an application jobtracker
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOBTRACKER):
            options.extend(['--jt', self.get(TaskOptions.CONFIG_KEY_MR_JOBTRACKER)])

        # comma separated files to be copied to the map reduce cluster
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_FILE):
            options.extend(['-files',
                            ",".join(self.get_list(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_FILE))])

        # comma separated jar files to include in the classpath
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_LIBJARS):
            options.extend(['-libjars',
                            ",".join(self.get_list(TaskOptions.CONFIG_KEY_MR_JOB_LIBJARS))])

        # comma separated archives to be unarchived on the compute machines
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_ARCHIVE):
            options.extend(['-archives',
                            ",".join(self.get_list(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_ARCHIVE))])

        return options

    def _command_options_(self):
        options = []
        return options

    def _update_list_config_(self, _key, *values):
        _inputs = self._config.get(self.name, _key) if self._config.has(self.name, _key) \
            else []
        _inputs.extend(values)
        return self._update_config_option_(_key, _inputs)

    def _update_config_option_(self, key, value):
        self._config.set(self.name, key, value)
        return self

    def with_config_option(self, key, value):
        """
        Adds or updates job configuration variable.
        In case java-based MR job options will be passed into configuration object
        :param key: variable name
        :param value: variable value
        :return:
        """
        return self._update_list_config_(TaskOptions.CONFIG_KEY_MR_JOB_CONF_OPTION,
                                         "{0}={1}".format(key, value))

    def use_jars(self, *libs):
        """
        Adds jar files to be included in the classpath
        :param libs: jar that should be placed in distributed cache
        and will be made available to all of the job's task attempts.
        :return:
        """
        return self._update_list_config_(TaskOptions.CONFIG_KEY_MR_JOB_LIBJARS, *libs)

    def cache_files(self, *files):
        """
        Adds files which will be copied to the Map/Reduce cluster
        :param files:  The list of files that need to be added to distributed cache
        :return:
        """
        return self._update_list_config_(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_FILE, *files)

    def cache_archives(self, *archives):
        """
        Adds archives which will be copied to the Map/Reduce cluster
        :param files:  The list of archives that need to be added to distributed cache
        :return:
        """
        return self._update_list_config_(TaskOptions.CONFIG_KEY_MR_JOB_CACHE_ARCHIVE, *archives)

    def has_option(self, key):
        """
        Checks if job configuration contains specified option
        :param key: option name
        :return: True option value
        """
        return self._config.has(self.name, key)

    def get(self, key, required=False):
        """
        Gets the value of the specified configuration option.
        :param key: option name
        :param required: Boolean flag, True is option is required
        :return: option value or None in case option was not found and option is not required.
        ConfigurationError will be thrown in case required option was not found within current
        configuration
        """
        return self._config.require(self.name, key) if required \
            else self._config.get(self.name, key)

    def get_list(self, key, required=False):
        """
        Gets the value of the specified configuration option property as a list

        :param key: option name
        :param required: True is option is required
        :return: property value as a list of strings or None in case option was not found
         and option is not required.
        ConfigurationError will be thrown in case required option was not found within current
        configuration
        """
        return self._config.require_list(section=self.name,
                                         key=key) if required \
            else self._config.get_list(section=self.name,
                                       key=key)


class StreamingJob(MapReduce):
    """
    Configures and runs streaming map-reduce job
    """

    def __init__(self, config, name, jar, executor):
        super(StreamingJob, self).__init__(
            name=name,
            config=config,
            executable=jar,
            executor=executor,
            main_class=None)

    def _mapper_option_(self):
        return ['-mapper', self.get(TaskOptions.CONFIG_KEY_MR_JOB_MAPPER_CLASS, required=True)]

    def with_number_of_reducers(self, reducer_num):
        """
        Sets number of ussng reducer
        :param reducer_num: number of reducer
        :type reducer_num: int, str
        :return:
        """
        return self._update_config_option_(TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_NUM,
                                           str(reducer_num))

    def _reducer_options_(self):
        """
        To be backward compatible, Hadoop Streaming supports the "-reducer NONE" option,
        which is equivalent to "-D mapreduce.job.reduces=0".

        :return:
        """
        _reducer_options = ['-reducer',
                            self.get(TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_CLASS) or 'NONE']
        if self.is_map_only_job():
            _reducer_options.extend(['-numReduceTasks', '0'])
        elif self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_NUM):
            _reducer_options.extend(['-numReduceTasks',
                                     self.get(TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_NUM)])
        return _reducer_options

    def _configure_inputs(self):
        _inputs = [("-input", _job_input) for _job_input in
                   self.get_list(TaskOptions.CONFIG_KEY_MR_JOB_INPUT_DIR, required=True)]
        return [item for t in _inputs for item in t]

    def _configure_outputs(self):
        return ['-output', self.get(TaskOptions.CONFIG_KEY_MR_JOB_OUTPUT_DIR, required=True)]

    def _command_options_(self):
        """
        Configures command-line command to run MR job
        :return: job parameters
        """
        arguments = []
        arguments.extend(self._mapper_option_())
        arguments.extend(self._reducer_options_())
        arguments.extend(self._configure_inputs())
        arguments.extend(self._configure_outputs())
        # OPTIONAL : set MR job inputformat
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_INPUT_FORMAT):
            arguments.extend(['-inputformat',
                              self.get(TaskOptions.CONFIG_KEY_MR_JOB_INPUT_FORMAT)])

        # OPTIONAL : set MR job outputformat
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_OUTPUT_FORMAT):
            arguments.extend(['-outputformat',
                              self.get(TaskOptions.CONFIG_KEY_MR_JOB_OUTPUT_FORMAT)])

        # partitioner
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_PARTITIONER_CLASS):
            arguments.extend(['-partitioner',
                              self.get(TaskOptions.CONFIG_KEY_MR_JOB_PARTITIONER_CLASS)])

        # combiner
        if self.has_option(TaskOptions.CONFIG_KEY_MR_JOB_COMBINE_CLASS):
            arguments.extend(['-combiner ',
                              self.get(TaskOptions.CONFIG_KEY_MR_JOB_COMBINE_CLASS)])

        # -cmdenv
        if self.has_option(TaskOptions.CONFIG_KEY_ENVIRONMENT):
            arguments.extend(["-cmdenv {0}".format(att)
                              for att in self.get_list(TaskOptions.CONFIG_KEY_ENVIRONMENT)])

        return arguments

    def environment_variable(self, key, value):
        """
        Sets environment variable to streaming commands
        :param key: variable name
        :param value: variable value
        :return:
        """
        self._update_list_config_(TaskOptions.CONFIG_KEY_ENVIRONMENT, "{0}={1}".format(key, value))
        return self

    def is_map_only_job(self):
        """

        :return: True in case mapper only Hadoop MapReduce Jobs
        """
        _key = TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_CLASS
        return not self.has_option(_key) or self.get(_key, required=False) is 'NONE'

    def process_with(self, mapper, reducer=None, reducer_num=-1):
        """

        :param mapper: mapper to use
        :param reducer: reducer to use
        :param reducer_num:  the number of reduce tasks
        :return:
        """
        self.map_with(mapper)
        self.reduce_with(reducer, reducer_num if reducer else -1)
        return self

    def map_with(self, mapper):
        """
        Sets the Mapper for the job. Mapper file will be added to distributed cache
        :param mapper: executable or JavaClassName
        :return:
        """
        _exists = os.path.exists(mapper)
        self._config.set(section=self.name,
                         key=TaskOptions.CONFIG_KEY_MR_JOB_MAPPER_CLASS,
                         value=(os.path.basename(mapper)) if _exists else mapper)
        if _exists:
            self.cache_files(mapper)
        return self

    def reduce_with(self, reducer, reducer_num=-1):
        """
        Sets the Reducer for the job.  Reducer file will be added to distributed cache
        :param reducer: executable or JavaClassName
        :param reducer_num: number of reduce tasks for the job.
        reducer_num param will be ignored in case it's value is less then 0
        :return:
        """
        if reducer:
            _exists = os.path.exists(reducer)
            _reducer_filename = os.path.basename(reducer)
            self._config.set(section=self.name,
                             key=TaskOptions.CONFIG_KEY_MR_JOB_REDUCER_CLASS,
                             value=_reducer_filename if _exists else reducer)
            if _exists:
                self.cache_files(reducer)

            if reducer_num != -1:
                self.with_number_of_reducers(reducer_num)

        return self

    def take(self, input_path):
        """
        Adds a path to the list of inputs for the map-reduce job.
        :param input_path:  the path of the input directories/files for the map-reduce job.
        :return:
        """
        self._update_list_config_(TaskOptions.CONFIG_KEY_MR_JOB_INPUT_DIR, input_path)
        return self

    def use(self,
            inputformat=None,
            outputformat=None,
            combiner=None,
            partitioner=None):
        """

        :param inputformat: JavaClassName.  Input-specification for the job.
        For streaming job class you supply should return key/value pairs of Text class.
        Required for Java based MR job

        :param outputformat: JavaClassName. Output-specification for thee job.
        For streaming job class you supply should take key/value pairs of Text class.
        Required for Java based MR job

        :param combiner: streamingCommand or JavaClassName. Combiner for the job, u
        sed as an optimization for mapper output

        :param partitioner: JavaClassName. Partitioner for the job.
        Class that determines which reduce a key is sent to

        """
        if inputformat:
            self._config.set(self.name,
                             TaskOptions.CONFIG_KEY_MR_JOB_INPUT_FORMAT,
                             inputformat)
        if outputformat:
            self._config.set(self.name,
                             TaskOptions.CONFIG_KEY_MR_JOB_OUTPUT_FORMAT,
                             outputformat)
        if combiner:
            self._config.set(self.name,
                             TaskOptions.CONFIG_KEY_MR_JOB_COMBINE_CLASS,
                             combiner)
        if partitioner:
            self._config.set(self.name,
                             TaskOptions.CONFIG_KEY_MR_JOB_PARTITIONER_CLASS,
                             partitioner)
        return self

    def save(self, output):
        """
        Sets the output directory for the map-reduce job.
        :param output: output directory
        :return:
        """
        self._config.set(section=self.name,
                         key=TaskOptions.CONFIG_KEY_MR_JOB_OUTPUT_DIR,
                         value=output)
        return self


class MapReduceJob(MapReduce):
    def __init__(self, name, config, jar, main_class, executor):
        super(MapReduceJob, self).__init__(name=name, config=config,
                                           executable=jar,
                                           executor=executor,
                                           main_class=main_class)

    def with_arguments(self, *args):
        """
        Sets specific argument to CLI
        """
        return self._update_list_config_(TaskOptions.COMMAND_ARGS, *args)

    def _command_options_(self):
        return self.get_list(TaskOptions.COMMAND_ARGS) \
            if self.has_option(TaskOptions.COMMAND_ARGS)else []


class TaskOptions(MapReduce):
    #name of mapreduce job
    CONFIG_KEY_MR_JOB_NAME = "mapreduce.job.name"
    #use value for given property '-D key=value'
    CONFIG_KEY_MR_JOB_CONF_OPTION = "job.config"
    #specify an application configuration file
    CONFIG_KEY_MR_JOB_CONF_FILE = "job.config.file"
    #specify a job tracker
    CONFIG_KEY_MR_JOBTRACKER = "jobtracker"
    #specify the number of reducers
    CONFIG_KEY_MR_JOB_REDUCER_NUM = "mapreduce.job.reduces"
    #specify comma-separated jar files to include in the classpath
    CONFIG_KEY_MR_JOB_LIBJARS = 'libjars'
    #specify comma-separated files to be copied to the Map/Reduce cluster
    CONFIG_KEY_MR_JOB_CACHE_FILE = 'files'
    #specify comma-separated archives to be unarchived on the compute machines
    CONFIG_KEY_MR_JOB_CACHE_ARCHIVE = 'archives'

    #pass environment variable to streaming commands
    CONFIG_KEY_ENVIRONMENT = "environment.vars"
    #mapper executable
    CONFIG_KEY_MR_JOB_MAPPER_CLASS = "mapper"
    #reducer executable
    CONFIG_KEY_MR_JOB_REDUCER_CLASS = "reducer"
    #combiner executable for map output
    CONFIG_KEY_MR_JOB_COMBINE_CLASS = "combiner"
    #class that determines which reduce a key is sent to
    CONFIG_KEY_MR_JOB_PARTITIONER_CLASS = "partitioner"
    #Class you supply should return key/value pairs of Text class.
    # If not specified, TextInputFormat is used as the default
    CONFIG_KEY_MR_JOB_INPUT_FORMAT = 'inputformat'
    #Class you supply should take key/value pairs of Text class.
    # If not specified, TextOutputformat is used as the default
    CONFIG_KEY_MR_JOB_OUTPUT_FORMAT = 'outputformat'
    #input location for mapper
    CONFIG_KEY_MR_JOB_INPUT_DIR = 'input'
    #output location for reducer
    CONFIG_KEY_MR_JOB_OUTPUT_DIR = 'output'

    #args for CLI
    COMMAND_ARGS = "args"

    #keus only for simple mapreduce job
    KEYS_FOR_MAPREDUCE = [COMMAND_ARGS]

    #keys only for streaming mapreduce job
    KEYS_FOR_STREAMING_JOB = [CONFIG_KEY_ENVIRONMENT, CONFIG_KEY_MR_JOB_MAPPER_CLASS,
                              CONFIG_KEY_MR_JOB_REDUCER_CLASS, CONFIG_KEY_MR_JOB_COMBINE_CLASS,
                              CONFIG_KEY_MR_JOB_INPUT_FORMAT, CONFIG_KEY_MR_JOB_OUTPUT_FORMAT,
                              CONFIG_KEY_MR_JOB_INPUT_DIR, CONFIG_KEY_MR_JOB_OUTPUT_DIR]


class JobStatus(object):
    """
    Describes the current status of a job.
    """

    COUNTER_SECTION = 'COUNTER'
    LOG = get_logger("MapReduceJobStatus")
    CLI_COMMAND = 'hadoop job'

    def __init__(self, job_id, executor=execute_shell_command):
        super(JobStatus, self).__init__()
        self.job_id = job_id
        self._executor = executor
        self.job_stats = None

    def state(self):
        """

        Returns the current state of the Job.
        :return: string value for job state.
        Possible values : FAILED, KILLED, PREP, RUNNING, SUCCEEDED
        """
        return self.stats()['Job state']

    @staticmethod
    def job_id(stderr):
        """
        Parses MR job stderr to get job id.
        :return: job id
        """
        _job_id = None
        for line in stderr.splitlines():
            if 'Running job:' in line:
                _job_id = str(line).rsplit(':', 1)[1].strip()
                JobStatus.LOG.info("Job id : {0}".format(_job_id))
                break
        if not _job_id:
            JobStatus.LOG.info("Cannot get job id")
        return _job_id

    def counters(self):
        """
        Gets the counters for this job.

        :return: all job counters in format {counter_group :{counter_name : counter_value}}
        """
        return self.stats()[JobStatus.COUNTER_SECTION]

    def counter(self, group, counter):
        """
        Gets the value of the specific job counter.
        :param group:
        :param counter:
        :return: the value for the specific counter
        """
        _counters = self.counters()
        return int(_counters[group][counter]) if group in _counters and counter in _counters[group] else None

    def stats(self):
        """
        Gets aggregate job statistics, which includes:
        - job id
        - job file
        - job tracking URL
        - number of maps/reduces
        - map()/reduce() completion
        - job state
        - reason for failture
        - job counters
        - etc
        :return: job details
        """
        if not self.job_stats:
            _result = self._executor(self.CLI_COMMAND, '-status', self.job_id)
            _result.if_failed_raise(CommandException("cannot get map reduce job status"))
            self._parse_stdout_(_result.stdout)
        return self.job_stats

    def is_failed(self):
        """
        Checks if the job failed.

        :return:
        """
        return 'FAILED' == self.state()

    def is_killed(self):
        """
        Checks if the job process was killed.

        :return:
        """
        return 'KILLED' == self.state()

    def is_succeeded(self):
        """
        Checks if the job completed successfully.

        :return:
        """
        return self.state() == 'SUCCEEDED'

    def is_running(self):
        """
        Checks if the job is finished or not.

        :return: True if the job has running or prep state
        """
        return self.state() in ['PREP', 'RUNNING']

    def failure_reason(self):
        """
        Gets any available info on the reason of failure of the job.

        :return:  diagnostic information on why a job might have failed.
        """
        return None if not self.is_failed() else self.stats()['reason for failure']

    def _parse_stdout_(self, stream):
        """
        Parses hadoop jar -status <job_id> output stream to get job stats
        :param stream: stream containing job stats data
        :return: dictionary containing job stats
        """
        _counter_group = None
        _job_metrics = {JobStatus.COUNTER_SECTION: {}}
        for line in stream.splitlines():
            is_counter = re.match('\A\t\t\w', line)
            is_counter_header = not is_counter and re.match('\A\t\w', line)
            key_value = [part.strip() for part in line.split("=" if is_counter else ":", 1)]
            if is_counter_header:
                _counter_group = line.strip()
            elif is_counter:
                if not _counter_group in _job_metrics[JobStatus.COUNTER_SECTION]:
                    _job_metrics[JobStatus.COUNTER_SECTION][_counter_group] = {}
                _job_metrics[JobStatus.COUNTER_SECTION][_counter_group][key_value[0]] = key_value[1]
            elif len(key_value) > 1:
                _job_metrics[key_value[0]] = key_value[1]
        self.job_stats = _job_metrics
