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
Spark client.

Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in Java, Scala and Python, and an optimized engine that
supports general execution graphs.

This client provides Python wrapper for Spark command-line interface

SPARK JOB EXAMPLES :

Runs Spark job using configuration file :
        SparkApplication.load_preconfigured_job(name="spark_app",
                                                config=Configuration.load("../config_file.ini",
                                                readonly=False)).run()

        'config_file.ini' contains of :
        [spark_app]
        application.jar=test.jar
        master=local[10]
        class=test.SparkApp
        name=test_app
        jars=lib001.jar
            lib002.jar
        files=dim001.cache.txt
            dim002.cache.txt
        properties-file=spark.app.configs
        conf=spark.app.name=test_app
            spark.executor.memory=512m

    Will be transformed to next Spark CLI command :
    spark-submit --master local[10] --class test.SparkApp --name test_app --jars lib001.jar,lib002.jar
     --files dim001.cache.txt,dim002.cache.txt --properties-file spark.app.configs
     --conf "spark.app.name=test_app spark.executor.memory=512m" test.jar

Runs Spark job using yarn client mode :
        SparkApplication().master(SparkMaster.yarn_client()).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master yarn-client --class Main --name Spark application_jar

Runs Spark job using yarn cluster mode :
        SparkApplication().master(SparkMaster.yarn_cluster()).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master yarn-cluster --class Main --name Spark application_jar

Runs Spark job using mesos mode :
        SparkApplication().master(SparkMaster.mesos(host="localhost", port=5050, use_zookeeper=True)).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master mesos://zk://localhost:5050 --class Main --name Spark application_jar

    SparkApplication().master(SparkMaster.mesos(host="localhost", port=5050, use_zookeeper=False)).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master mesos://localhost:5050 --class Main --name Spark application_jar

Runs Spark job using standalone mode :
        SparkApplication().master(SparkMaster.standalone(host="localhost", port=5050)).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master spark://localhost:5050 --class Main --name Spark application_jar

Runs Spark job using local mode :
        SparkApplication().master(SparkMaster.local(workers=5)).\
        application("application_jar", main_class="Main", app_name="Spark").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master local[5] --class Main --name Spark application_jar

Runs Spark job with config file :
        SparkApplication().master(SparkMaster.local(workers=5)).\
        application("application_jar", main_class="Main", app_name="Spark").\
        config_file(path="./config_file").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master local[5] --class Main --name Spark
    --properties-file ./config_file application_jar

Runs Spark job with config option :
        SparkApplication().master(SparkMaster.local(workers=5)).\
        application("application_jar", main_class="Main", app_name="Spark").\
        with_config_option("spark.app.name", "test_app").run()

    Will be transformed to next Spark CLI command :
    spark-submit --master local[5] --class Main --name Spark
    --conf "spark.app.name=test_app" application_jar

"""

import uuid
from merlin.common.configurations import Configuration
from merlin.common.shell_command_executor import execute_shell_command
from merlin.common.logger import get_logger


class SparkApplication(object):
    """
    Wrapper for spark-submit command line utility.
    Provides simple DSL to configure and launch Spark Application
    """
    LOG = get_logger("Spark")
    SHELL_COMMAND = "spark-submit"

    def __init__(self, config=None, name=None, executor=execute_shell_command):
        """

        :param config: configurations
        :param name: name of the config section containing specific application configurations
        :param executor: he interface used by the client to launch Spark Application.
        """
        super(SparkApplication, self).__init__()
        self.executor = executor
        self._configs = config if config else Configuration.create()
        self.name = name if name \
            else "SPARK_JOB_{0}".format(uuid.uuid4())

    @staticmethod
    def load_preconfigured_job(name=None, config=None, executor=execute_shell_command):
        """
        Creates wrapper for spark-submit command line utility. Configure it with options
        :param config: spark job configurations
        :param name: spark job identifier.
             Will be used as a name of the section with job-specific configurations.
        :param executor:
        :return:
        """
        SparkApplication.LOG.info("Loading Spark Job from configuration")

        return SparkApplication(name=name, config=config, executor=executor)

    def run(self, *args):
        """
        Submits spark application.
        :param args: Arguments passed to the main method of your main class, if any
        """

        return self._fire_job(verbose=False, args=args)

    def _fire_job(self, verbose=False, args=None):
        _options = []
        _options.extend(self._configure_spark_options())
        if verbose:
            _options.append("--verbose")
        _options.append(self._configs.require(self.name, TaskOptions.SPARK_APP_CONFIG_APPLICATION_JAR))
        if args:
            _options.extend(str(arg) for arg in args)

        return SparkJobStatus(self.executor(self.SHELL_COMMAND, *_options))

    def debug(self, *args):
        """
        Submits spark application in a verbose mode.
        :param args: Arguments passed to the main method of your main class, if any
        """
        return self._fire_job(verbose=True, args=args)

    def master(self, master):
        """
        Sets the cluster manager
        :param master: master URL of the clusters
        :return:
        """
        self._configs.set(section=self.name,
                          key=TaskOptions.SPARK_APP_CONFIG_MASTER,
                          value=master)
        return self

    def application(self, application_jar, main_class=None, app_name=None):
        """
        Configures Spark application
        :param application_jar: Path to a bundled jar including your application and all dependencies
        :param main_class: Java or Scala classname. Application entry point
        :param app_name:
        :return:
        """
        self.application_jar(application_jar)
        self.main_class(main_class)
        self.application_name(app_name)
        return self

    def config_file(self, path):
        """
        Configures Spark app to load default properties from file.
        :param path: Path to a file from which to load extra properties.
        If not specified, this will look for conf/spark-defaults.conf.
        :return:
        """
        self._configs.set(section=self.name,
                          key=TaskOptions.SPARK_APP_CONFIG_PROPERTIES_FILE,
                          value=path)
        return self

    def with_config_option(self, key, value):
        """
        Supplies configuration values at runtime.
        According to specification,
        configuration values explicitly set on a SparkConf take the highest precedence,
        then flags passed to spark-submit, then values in the defaults file.
        :param key: option name.
            see https://spark.apache.org/docs/latest/configuration.html for supported properties
        :param value:  option value
        :return:
        """
        _key_value_pair = "{key}={value}".format(key=key, value=value)

        self._configs.update_list(self.name,
                                  TaskOptions.SPARK_APP_CONFIG_OPTIONS,
                                  _key_value_pair)

        return self

    def main_class(self, main_class):
        """
        Sets spark application's main class (for Java / Scala apps).
        :param main_class:
        :return:
        """
        if main_class:
            self._configs.set(section=self.name,
                              key=TaskOptions.SPARK_APP_CONFIG_MAIN_CLASS,
                              value=main_class)
        return self

    def application_jar(self, app_jar):
        """
        Sets path to a bundled jar including application with all dependencies.
        :param app_jar: path to application jar.
        :return:
        """
        if app_jar:
            self._configs.set(section=self.name,
                              key=TaskOptions.SPARK_APP_CONFIG_APPLICATION_JAR,
                              value=app_jar)
        return self

    def application_name(self, name):
        """
        Sets application's name. This will appear in the UI and in log data.
        :param name:  A name of Spark application.
        :return:
        """
        if name:
            self._configs.set(section=self.name,
                              key=TaskOptions.SPARK_APP_CONFIG_APP_NAME,
                              value=name)
        return self

    def classpath(self, *jar_files):
        """
        Specifies the list of local jars to include on the driver and executor classpaths.
        :param jar_files: jar files to be included to application classpath
        :return:
        """
        self._configs.update_list(self.name,
                                  TaskOptions.SPARK_APP_CONFIG_JARS,
                                  *jar_files)
        return self

    def pythonpath(self, *pyfiles):
        """
        Specifies the list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.
        :param pyfiles:
        :return:
        """
        self._configs.update_list(self.name,
                                  TaskOptions.SPARK_APP_CONFIG_PYFILES,
                                  *pyfiles)
        return self

    def add_files(self, *files):
        """
        Adds files to be placed in the working directory of each executor
        :param files:
        :return:
        """
        self._configs.update_list(self.name,
                                  TaskOptions.SPARK_APP_CONFIG_FILES,
                                  *files)
        return self

    def _configure_spark_options(self):
        """
           Adds next args to command :
           --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
           --deploy-mode DEPLOY_MODE   Where to run the driver program: either "client" to run
                                      on the local machine, or "cluster" to run inside cluster.
           --class CLASS_NAME          Your application's main class (for Java / Scala apps).
           --name NAME                 A name of your application.
           --jars JARS                 Comma-separated list of local jars to include on the driver
                                      and executor classpaths.
           --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
                                      on the PYTHONPATH for Python apps.
           --files FILES               Comma-separated list of files to be placed in the working
                                      directory of each executor.
           --properties-file FILE      Path to a file from which to load extra properties. If not
                                      specified, this will look for conf/spark-defaults.conf.
          --conf PROP=VALUE           Arbitrary Spark configuration property.


        :return:
        """
        _options = []
        _section = self.name
        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_MASTER):
            _options.extend(["--master", self._configs.get(_section, TaskOptions.SPARK_APP_CONFIG_MASTER)])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_MAIN_CLASS):
            _options.extend(["--class", self._configs.get(_section, TaskOptions.SPARK_APP_CONFIG_MAIN_CLASS)])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_APP_NAME):
            _options.extend(["--name", self._configs.get(_section, TaskOptions.SPARK_APP_CONFIG_APP_NAME)])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_JARS):
            _options.extend(["--jars",
                             ",".join(self._configs.get_list(_section, TaskOptions.SPARK_APP_CONFIG_JARS))])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_PYFILES):
            _options.extend(["--py-files",
                             ",".join(self._configs.get_list(_section, TaskOptions.SPARK_APP_CONFIG_PYFILES))])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_FILES):
            _options.extend(["--files",
                             ",".join(self._configs.get_list(_section, TaskOptions.SPARK_APP_CONFIG_FILES))])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_PROPERTIES_FILE):
            _options.extend(["--properties-file",
                             self._configs.get(_section, TaskOptions.SPARK_APP_CONFIG_PROPERTIES_FILE)])

        if self._configs.has(_section, TaskOptions.SPARK_APP_CONFIG_OPTIONS):
            _options.extend(["--conf",
                             "\"{0}\"".format(" ".join(
                                 self._configs.get_list(_section, TaskOptions.SPARK_APP_CONFIG_OPTIONS))
                             )])

        return _options


class TaskOptions(SparkApplication):

    #path to a file from which to load extra properties. If not
    #specified, this will look for conf/spark-defaults.conf.
    SPARK_APP_CONFIG_PROPERTIES_FILE = "properties-file"
    #arbitrary Spark configuration property PROP=VALUE.
    SPARK_APP_CONFIG_OPTIONS = "conf"
    #Comma separated list of archives to be extracted into the
    #working directory of each executor.
    SPARK_APP_CONFIG_ARCHIVES = "archives"
    #comma-separated list of files to be placed in the working
    #directory of each executor.
    SPARK_APP_CONFIG_FILES = "files"
    #comma-separated list of .zip, .egg, or .py files to place
    #on the PYTHONPATH for Python apps.
    SPARK_APP_CONFIG_PYFILES = "pyfiles"
    #comma-separated list of local jars to include on the driver
    #and executor classpaths.
    SPARK_APP_CONFIG_JARS = "jars"
    #a name of your application.
    SPARK_APP_CONFIG_APP_NAME = "name"
    #your application's main class (for Java / Scala apps).
    SPARK_APP_CONFIG_MAIN_CLASS = "class"
    #spark://host:port, mesos://host:port, yarn, or local.
    SPARK_APP_CONFIG_MASTER = "master"

    SPARK_APP_CONFIG_APPLICATION_JAR = "application.jar"
    

class SparkMaster(object):
    def __init__(self, url):
        super(SparkMaster, self).__init__()
        self.url = url

    @staticmethod
    def yarn_client():
        """
        Uses Yarn cluster in a client mode.
        :return: master url
        """
        return "yarn-client"

    @staticmethod
    def yarn_cluster():
        """
        Uses Yarn cluster in a cluster mode.

        :return: master url
        """
        return "yarn-cluster"

    @staticmethod
    def mesos(host, port=5050, use_zookeeper=False):
        """
        Uses mesos cluster
        :param host:
        :param port:
        :return:  master url
        """
        return ("mesos://{0}:{1}".format(host, port) if not use_zookeeper
                else "mesos://zk://{0}:{1}").format(host, port)

    @staticmethod
    def standalone(host, port=7077):
        """
        Uses Spark standalone cluster master
        :param host:
        :param port:
        :return:  master url
        """
        return "spark://{0}:{1}".format(host, port)

    @staticmethod
    def local(workers='*'):
        """
        Will run spark locally.
        :param workers: number of worker threads.
                        By default will use as many worker threads as logical cores on your machine.
        :return:  master url
        """
        return "local" if not workers else "local[{0}]".format(workers)
    

class SparkJobStatus(object):
    def __init__(self, joboutput):
        super(SparkJobStatus, self).__init__()
        self.joboutput = joboutput

    def is_ok(self):
        return self.joboutput.is_ok()

    def stdout(self):
        return self.joboutput.stdout

    def stderr(self):
        return self.joboutput.stderr