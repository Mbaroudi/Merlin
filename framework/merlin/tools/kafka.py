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
Kafka client.
Kafka is a distributed, partitioned, replicated commit log service.
It provides the functionality of a messaging system, but with a unique design.

"""

from merlin.common.logger import get_logger
from merlin.common.shell_command_executor import  execute_shell_command


class Kafka():
    """
    Wrapper for Kafka command line scripts
    """
    RUN_SHELL = "kafka-run-class.sh"

    LOG = get_logger("Kafka")

    @staticmethod
    def run_consumer(name, args, executor=execute_shell_command, kafka_run_class=RUN_SHELL):
        """
        Runs specific consumer. Executing command:
        kafka-run-class.sh {name} {configs}
        :param name:
        :param args:
        :param executor:
        :return:
        """
        command = "{0} {1}".format(kafka_run_class, name)
        if args:
            command += Kafka.__get_configs(args)
        return Kafka.__run(executor, command)

    @staticmethod
    def __get_configs(configs):
        command = ""
        if isinstance(configs, list):
            for value in configs:
                command += " {0}".format(value)
        elif isinstance(configs, str):
            command += " {0}".format(configs)
        elif isinstance(configs, dict):
            for key, value in configs.iteritems():
                command += " {0} {1}".format(key, value)
        return command

    @staticmethod
    def __get_configs_topics(configs):
        command = ""
        if isinstance(configs, list):
            for value in configs:
                command += " --config {0}".format(value)
        elif isinstance(configs, str):
            for value in configs.split(","):
                command += " --config {0}".format(value)
        elif isinstance(configs, dict):
            for key, value in configs.iteritems():
                command += " --config {0}={1}".format(key, value)
        return command

    @staticmethod
    def run_producer(name, args, executor=execute_shell_command, kafka_run_class=RUN_SHELL):
        """
        Runs specific producer. Executing command:
        kafka-run-class.sh {name} {configs}
        :param name:
        :param args:
        :param executor:
        :return:
        """
        command = "{0} {1}".format(kafka_run_class, name)
        if args:
            command += Kafka.__get_configs(args)
        return Kafka.__run(executor, command)

    @staticmethod
    def start_broker(path_to_config, executor=execute_shell_command, kafka_run_class="kafka-server-start.sh"):
        """
        Runs broker using configuration file. Executing command:
        kafka-server-start.sh {path_to_config}
        :param path_to_config:
        :param executor:
        :return:
        """
        command = "{0} {1}".format(kafka_run_class, path_to_config)
        return Kafka.__run(executor, command)

    @staticmethod
    def stop_broker(path_to_config, executor=execute_shell_command, kafka_run_class="kafka-server-stop.sh"):
        """
        Runs broker using configuration file. Executing command:
        kafka-server-stop.sh {path_to_config}
        :param path_to_config:
        :param executor:
        :return:
        """
        command = "{0} {1}".format(kafka_run_class, path_to_config)
        return Kafka.__run(executor, command)

    @staticmethod
    def create_topic(name, replication_factor=None, replica_assignment=None, partitions=1,
                     zookeeper_host=None, args=None, executor=execute_shell_command, kafka_run_class=RUN_SHELL):
        """
        Creates topic
        :param name:
        :param replication_factor:
        :param replica_assignment:
        :param partitions:
        :param zookeeper_host:
        :param args:
        :param executor:
        :return:
        """
        command = "{0} kafka.admin.TopicCommand --create --zookeeper {2} --topic {1} --partitions {3}" \
            .format(kafka_run_class, name, zookeeper_host, partitions)
        if replication_factor:
            command += " --replication-factor {0}".format(replication_factor)
        if replica_assignment:
            command += " --replication-assignment {0}".format(replica_assignment)
        if args:
            command += Kafka.__get_configs_topics(args)
        Kafka.__run(executor, command)
        return Topic(name, zookeeper_host, executor)

    @staticmethod
    def get_list_topics(zookeeper_host=None, executor=execute_shell_command, kafka_run_class=RUN_SHELL):
        """
        Returns existing list of topics on zookeeper
        :param zookeeper_host:
        :param executor:
        :return:
        """
        command = "{0} kafka.admin.TopicCommand --zookeeper {1} --list" \
            .format(kafka_run_class, zookeeper_host)
        topics = []
        for t in Kafka.__run(executor, command).stdout.split('\n'):
            topics.append(Topic(t, zookeeper_host))
        return topics

    @staticmethod
    def __run(executor, command):
        Kafka.LOG.info("Executing Kafka command: {0}".format(command))
        return executor(command)


class Topic():
    LOG = get_logger("Topic")

    def __init__(self, name, zookeeper_host, executor=execute_shell_command):
        """
        Wrapper for Kafka command #./bin/kafka-run-class.sh kafka.admin.TopicCommand
        :param name:
        :param zookeeper_host:
        :param executor:
        :return:
        """
        self.name = name
        self.zookeeper_host = zookeeper_host
        self._executor = executor

    def get_metadata(self, kafka_run_class=Kafka.RUN_SHELL):
        """
        Returns metadata of topic. Executing command:
        #./bin/kafka-run-class.sh kafka.admin.TopicCommand --topic {name} --describe --zookeeper {host:port}
        :return:
        """
        return self.__run("--topic {0} --describe".format(self.name), kafka_run_class).stdout
    
    def add_config(self, key, value, kafka_run_class=Kafka.RUN_SHELL):
        """
        Adds config to topic. Executing command:
        #./bin/kafka-run-class.sh kafka.admin.TopicCommand --topic {name} --alter
        --zookeeper {host:port} config {key=value}
        :param key:
        :param value:
        :return:
        """

        return self.__run("--topic {0} --alter config {1}={2}".format(self.name, key, value), kafka_run_class)
    
    def delete_config(self, key, value, kafka_run_class=Kafka.RUN_SHELL):
        """
        Deletes config from topic. Executing command:
        #./bin/kafka-run-class.sh kafka.admin.TopicCommand --topic {name} --alter
        --zookeeper {host:port} deleteConfig {key=value}
        :param key:
        :param value:
        :return:
        """

        return self.__run("--topic {0} --alter deleteConfig {1}={2}".format(self.name, key, value), kafka_run_class)

    def delete(self, kafka_run_class=Kafka.RUN_SHELL):
        """
        Deletes topic. Executing command:
        #./bin/kafka-run-class.sh kafka.admin.TopicCommand --topic {name} --delete
        --zookeeper {host:port}
        :return:
        """

        return self.__run("--topic {0} --delete".format(self.name), kafka_run_class)

    def is_exists(self, kafka_run_class=Kafka.RUN_SHELL):
        """
        Returns True if topic exist else - False. Executing command:
        #./bin/kafka-run-class.sh kafka.admin.TopicCommand --topic {name} --list --zookeeper {host:port}
        :return:
        """
        result = self.__run("--list", kafka_run_class)
        topics = result.stdout.split('\n')
        return str(self.name) in topics

    def __run(self, command, kafka_run_class):
        Topic.LOG.info("Executing Topic command")
        result = self._executor("{0} kafka.admin.TopicCommand".format(kafka_run_class), "--zookeeper",
                                self.zookeeper_host, command)
        return result

