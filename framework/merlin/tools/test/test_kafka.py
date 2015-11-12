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

import unittest2
from unittest2.case import expectedFailure

from merlin.tools.kafka import Kafka, Topic
from merlin.common.test_utils import mock_executor


class TestKafka(unittest2.TestCase):
    def setUp(self):
        super(TestKafka, self).setUp()

    def test_run_consumer(self):
        _command = "kafka-run-class.sh Consumer --zookeeper lcoalhost"
        Kafka.run_consumer(name='Consumer', args={"--zookeeper": "lcoalhost"}, executor=mock_executor(expected_command=_command))

    def test_run_producer(self):
        _command = "kafka-run-class.sh Producer conf1 conf2"
        Kafka.run_producer(name='Producer', args=["conf1", "conf2"], executor=mock_executor(expected_command=_command))

    def test_create_topic(self):
        _command = "kafka-run-class.sh kafka.admin.TopicCommand --create --zookeeper master1.mycluster:2181" \
                   " --topic Topic --partitions 3 --replication-factor 1"
        Kafka.create_topic(name='Topic', replication_factor=1,
                           partitions=3, zookeeper_host="master1.mycluster:2181",
                           executor=mock_executor(expected_command=_command))

    def test_run_broker(self):
        _command = "kafka-server-start.sh path/to/config"
        Kafka.start_broker(path_to_config="path/to/config", executor=mock_executor(expected_command=_command))

    def test_stop_broker(self):
        _command = "kafka-server-stop.sh path/to/config"
        Kafka.stop_broker(path_to_config="path/to/config", executor=mock_executor(expected_command=_command))

    @expectedFailure
    def test_get_list_topics(self):
        _command = "kafka-run-class.sh kafka.admin.TopicCommand --zookeeper master1.mycluster:2181 --list"
        Kafka.get_list_topics(zookeeper_host="master1.mycluster:2181", executor=mock_executor(expected_command=_command))

    def test_get_metadta(self):
        _command = "kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --topic Topic --describe"
        topic = Topic(name="Topic", zookeeper_host="localhost:2181", executor=mock_executor(expected_command=_command))
        topic.get_metadata()

    def test_add_config(self):
        _command = "kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --topic Topic --alter config kye1=value1"
        topic = Topic(name="Topic", zookeeper_host="localhost:2181", executor=mock_executor(expected_command=_command))
        topic.add_config(key="kye1", value="value1")

    def test_delete_config(self):
        _command = "kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --topic Topic --alter deleteConfig kye1=value1"
        topic = Topic(name="Topic", zookeeper_host="localhost:2181", executor=mock_executor(expected_command=_command))
        topic.delete_config(key="kye1", value="value1")