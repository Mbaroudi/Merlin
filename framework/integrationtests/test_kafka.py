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
import socket

import threading
from time import sleep
import os
import uuid

import unittest2
from unittest2.case import skipUnless

from merlin.fs.localfs import LocalFS
from merlin.tools.kafka import Kafka, Topic
import merlin.common.shell_command_executor as shell
from merlin.common.test_utils import has_command

PORT = 9010
TIME = 20
CLUSTER_NAME = "sandbox.hortonworks.com"


class KafkaThreadBroker(threading.Thread):
    def run(self):
        Kafka.start_broker(path_to_config=os.path.join(os.path.dirname(__file__), 'resources/kafka/server.properties'))


class KafkaThreadProducer(threading.Thread):
    def run(self):
        Kafka.run_producer(name='kafka.producer.ConsoleProducer', args=["--broker-list", "{0}:{1}".format(CLUSTER_NAME, PORT), "--topic", "test123"])


class KafkaThreadConsumer(threading.Thread):
    def run(self):
        Kafka.run_consumer(name='kafka.consumer.ConsoleConsumer', args=["--zookeeper sandbox.hortonworks.com:2181 --from-beginning --topic test123"])


@skipUnless(has_command("kafka-run-class.sh") and has_command("netstat"), "./kafka/bin should be set to $PATH and netstat should be installed")
class TestKafka(unittest2.TestCase):

    def test_broker(self):
        shell.execute_shell_command('fuser -k -n tcp {0}'.format(PORT))
        local = LocalFS("/tmp/kafka-test")
        if not local.exists():
            local.create_directory()
        thread = KafkaThreadBroker()
        thread.daemon = True
        thread.start()
        sleep(TIME)
        cmd = shell.execute_shell_command('netstat -lntu')
        self.assertTrue("9010" in cmd.stdout, cmd.stdout)
        local.delete_directory()
        shell.execute_shell_command('fuser -k -n tcp {0}'.format(PORT))

    def test_run_consumer(self):
        thread = KafkaThreadConsumer()
        thread.daemon = True
        thread.start()
        sleep(TIME)
        cmd = shell.execute_shell_command('ps aux | grep -i kafka')
        self.assertTrue("kafka.consumer.ConsoleConsumer --zookeeper sandbox.hortonworks.com:2181 --from-beginning --topic test123" in cmd.stdout, cmd.stdout)
        for stroke in cmd.stdout.split("\n"):
            if "kafka.consumer.ConsoleConsumer --zookeeper sandbox.hortonworks.com:2181 --from-beginning --topic test123" in stroke:
                shell.execute_shell_command('kill -9 {0}'.format(stroke.split()[1]))

    def test_run_producer(self):
        thread = KafkaThreadProducer()
        thread.daemon = True
        thread.start()
        sleep(TIME)
        cmd = shell.execute_shell_command('ps aux | grep -i kafka')
        self.assertTrue("kafka.producer.ConsoleProducer --broker-list {0}:{1} --topic test123".format(CLUSTER_NAME, PORT) in cmd.stdout, cmd.stdout)
        for stroke in cmd.stdout.split("\n"):
            if "kafka.producer.ConsoleProducer --broker-list {0}:{1} --topic test123".format(CLUSTER_NAME, PORT) in stroke:
                shell.execute_shell_command('kill -9 {0}'.format(stroke.split()[1]))

    def test_create_topic(self):
        name = uuid.uuid4()
        topic = Topic(name=name, zookeeper_host="sandbox.hortonworks.com:2181")
        self.assertFalse(topic.is_exists())
        Kafka.create_topic(name=name, replication_factor=1,
                           partitions=1,
                           zookeeper_host="sandbox.hortonworks.com:2181")
        self.assertTrue(topic.is_exists())
        self.assertTrue(str(name) in topic.get_metadata())
        topic.delete()
