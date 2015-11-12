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

from unittest2.case import skipUnless
from merlin.common.test_utils import has_command
from merlin.tools.webhcat import WebHCatalog, TableProperties
import merlin.common.shell_command_executor as shell

import unittest2


WEBHCAT_IS_INSTALLED_AND_ENABLED = True
HOST = "{0}:50111".format("sandbox.hortonworks.com")
USER = "root"


@skipUnless(has_command('hive') and WEBHCAT_IS_INSTALLED_AND_ENABLED, "Hive and WebHCatalog clients should be installed and enabled")
class TestWebHCatalog(unittest2.TestCase):
    @classmethod
    def setUpClass(cls):
        shell.execute_shell_command('hive -e \"drop database if EXISTS testdb CASCADE\"')
        shell.execute_shell_command('hive -e \"create database testdb\"')
        c = 'hive -e \"create table testdb.some_table(strings STRING) ' \
            'ROW FORMAT DELIMITED ' \
            'FIELDS TERMINATED BY \\",\\" ' \
            'STORED AS TEXTFILE\"'
        shell.execute_shell_command(c)

    def test_get_property(self):
        output = WebHCatalog(host=HOST, username=USER).table_properties(database="testdb",
                                                                        table="some_table").get_property("table")
        self.assertEquals(output, "some_table")

    def test_get_all_properties(self):
        output = WebHCatalog(host=HOST, username=USER).table_properties(database="testdb",
                                                                        table="some_table").properties_as_map()["table"]
        self.assertEquals(output, "some_table")

    def test_put_property(self):
        w = WebHCatalog(host=HOST, username=USER)
        result = TableProperties(database="testdb", table="some_table", webhcat=w).set_property("my")
        self.assertTrue(result)

    @classmethod
    def tearDownClass(cls):
        shell.execute_shell_command('hive -e \"drop database if EXISTS testdb\"')
