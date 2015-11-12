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

import os

import unittest2
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore

from merlin.tools.hive import Hive
from merlin.common.test_utils import mock_executor


class TestHive(unittest2.TestCase):
    def setUp(self):
        super(TestHive, self).setUp()

    def test___init__(self):
        hive = Hive("hello", lambda: "world")
        assert True if hive is not None else False

    def test_load_config(self):
        _command = "hive -e \"test\" --define A=B --define C=D --hiveconf hello=world " \
                   "--hivevar A=B --hivevar C=D --database hive"
        metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__), 'resources/hive/hive.ini'))
        hive = Hive.load_preconfigured_job(name='hive test',
                                           config=Configuration.load(
                                               metastore=metastore,
                                               readonly=False, accepts_nulls=True),
                                           executor=mock_executor(expected_command=_command)) \
            .with_hive_conf("hello", "world")
        hive.run()

    def test_add_hiveconf(self):
        _command = "hive -e \"test\" --hiveconf hello=world"
        hive = Hive.load_queries_from_string(query="test", executor=mock_executor(expected_command=_command)) \
            .with_hive_conf("hello", "world")
        hive.run()

    def test_add_hivevar(self):
        _command = "hive -e \"test\" --hivevar hello=world"
        hive = Hive.load_queries_from_string(query="test", executor=mock_executor(expected_command=_command)) \
            .add_hivevar("hello", "world")
        hive.run()

    def test_define_variable(self):
        _command = "hive -e \"test\" --define hello=world"
        hive = Hive.load_queries_from_string(query="test", executor=mock_executor(expected_command=_command)) \
            .define_variable("hello", "world")
        hive.run()

    def test_use_database(self):
        _command = "hive -e \"test\" --database hello"
        hive = Hive.load_queries_from_string(query="test", executor=mock_executor(expected_command=_command)) \
            .use_database("blabla") \
            .use_database("hello")
        hive.run()

    def test_with_auxpath(self):
        _command = "hive " \
                   "--auxpath dear user,hello "\
                   "-e \"test\" " \
                   "--define key=value " \
                   "--hivevar hello=user " \
                   "--database hello" \


        hive = Hive.load_queries_from_string(query="test", executor=mock_executor(expected_command=_command)) \
            .with_auxillary_jars("dear user,hello") \
            .add_hivevar("hello", "user") \
            .use_database("hello") \
            .define_variable("key", "value")
        hive.run()



