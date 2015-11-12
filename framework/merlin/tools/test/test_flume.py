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

from merlin.tools.flume import Flume
from merlin.common.test_utils import mock_executor


class TestFlume(unittest2.TestCase):
    def setUp(self):
        super(TestFlume, self).setUp()

    def test_agent(self):
        command = "flume-ng agent --name a1 --conf-file path/to/file"
        flume = Flume.agent(agent="a1",
                            conf_file="path/to/file",
                            executor=mock_executor(expected_command=command))
        flume.run()

    @expectedFailure
    def test_agent_failed(self):
        command = "flume-ng agent --name a1"
        flume = Flume.agent(agent="a1",
                            executor=mock_executor(expected_command=command))
        flume.run()

    def test_agent_with_options(self):
        command = "flume-ng agent --name a1 --conf-file path/to/file --conf path/to/dir " \
                  "--plugins-path path/to/plugins1,path/to/plugins2,path/to/plugins3 -X4=test"
        flume = Flume.agent(agent="a1",
                            conf_file="path/to/file",
                            executor=mock_executor(expected_command=command))
        flume.with_jvm_X_option(value="test", name=4).load_configs_from_dir("path/to/dir")
        flume.load_plugins_from_dirs(pathes=["path/to/plugins1,path/to/plugins2", "path/to/plugins3"])
        flume.run()