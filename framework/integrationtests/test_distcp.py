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

from unittest2 import TestCase, skipUnless

from merlin.tools.distcp import DistCp
import merlin.common.shell_command_executor as shell
from merlin.common.test_utils import has_command


@skipUnless(has_command('hadoop'), "Hadoop client should be installed")
class TestDistCp(TestCase):
    def setUp(self):
        super(TestDistCp, self).setUp()
        shell.execute_shell_command('hadoop fs', '-mkdir /tmp/foo')
        shell.execute_shell_command('hadoop fs', '-mkdir /tmp/bar')
        shell.execute_shell_command('hadoop fs', '-touchz /tmp/foo/test.txt')
        shell.execute_shell_command('hadoop fs', '-touchz /tmp/foo/test2.txt')

    def test_command(self):
        _host = "sandbox.hortonworks.com"
        cmd = DistCp().take(path="hdfs://{host}:8020/tmp/foo".format(host=_host)).copy_to(
            path="hdfs://{host}:8020/tmp/bar".format(host=_host)
        ).use(
            mappers=12
        ).update_destination(
            synchronize=True
        ).preserve_replication_number()\
            .preserve_block_size()\
            .preserve_checksum_type()\
            .preserve_group()\
            .preserve_checksum_type()\
            .preserve_user()\
            .run()

        self.assertEquals(cmd.status, 0, cmd.stderr)

        self.assertEquals(shell.execute_shell_command('hadoop', 'fs', '-test', '-e', '/tmp/bar/test.txt').status, 0)
        self.assertEquals(shell.execute_shell_command('hadoop', 'fs', '-test', '-e', '/tmp/bar/test2.txt').status, 0)

    def tearDown(self):
        shell.execute_shell_command('hadoop fs', '-rm -r /tmp/foo')
        shell.execute_shell_command('hadoop fs', '-rm -r /tmp/bar')
