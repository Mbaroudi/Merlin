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

from tempfile import NamedTemporaryFile
import uuid

import unittest2
from unittest2.case import skipUnless, expectedFailure

from merlin.common.exceptions import HiveCommandError
from merlin.common.shell_command_executor import execute_shell_command
import merlin.fs.cli.hdfs_commands as hdfs
from merlin.tools.hive import Hive
from merlin.common.test_utils import has_command
import merlin.common.shell_command_executor as shell


@skipUnless(has_command('hive'), "Hive client should be installed")
class TestExecuteQuery(unittest2.TestCase):
    
    @classmethod
    def setUpClass(cls):
        shell.execute_shell_command('hive -e "drop database if EXISTS testdb CASCADE;"')
    
    def test_execute_query_string(self):
        hive = Hive.load_queries_from_string("show tables").with_hive_conf("A", "B").add_hivevar("A", "B") \
            .define_variable("A", "B")
        res = hive.run()
        self.assertEqual(res.is_ok(), True)

    def test_create_database(self):
        db_exist = False
        try:
            msg = "create database if not EXISTS testdb;"
            self.assertRaises(HiveCommandError, Hive.load_queries_from_string("drop database testdb").run)

            self.assertTrue(Hive.load_queries_from_string(msg).run().is_ok(),
                            "create database Done")
            db_exist = True
            self.assertTrue(Hive.load_queries_from_string("show tables").use_database("testdb").run().is_ok(),
                            "check database in query")
        finally:
            if db_exist:
                Hive.load_queries_from_string("drop database testdb;").run()

    def test_create_table(self):
        files = self.temp_file(msg="hello,world")
        try:
            msg = "create database if not EXISTS testdb;"
            self.assertTrue(Hive.load_queries_from_string(msg).run().is_ok())
            msg = "create table some_table(strings STRING)" \
                  "ROW FORMAT DELIMITED " \
                  "FIELDS TERMINATED BY ','" \
                  "STORED AS TEXTFILE " \
                  "Location '/tmp/hive_table';"
            hive = Hive.load_queries_from_string(msg).use_database("testdb")
            self.assertTrue(hive.use_database("testdb").run().is_ok(), " create table")
            msg = "LOAD DATA LOCAL INPATH '{0}' OVERWRITE INTO TABLE some_table;".format(files)
            hive = Hive.load_queries_from_string(msg).use_database("testdb")
            self.assertTrue(hive.run().is_ok(), "load data in table")
            self.assertTrue(hdfs.is_file_exists("/tmp/hive_table"), "check tables")
        finally:
            Hive.load_queries_from_string("drop table some_table;").use_database("testdb").run()
            Hive.load_queries_from_string("drop database testdb;").use_database("testdb").run()
            self.delete_local(files)
            self.delete_file_in_hdfs()

    def test_equals_file_string(self):
        files = self.temp_file()
        try:
            expend_q = Hive.load_queries_from_string("show tables").use_database("default")
            expend_f = Hive.load_queries_from_file(files).use_database("default")
            self.assertEqual(expend_q.run().is_ok(), expend_f.run().is_ok())
        finally:
            self.delete_local(files)

    def test_exist_table_file(self):
        files, msg = self.create_database()
        msg_f = self.temp_file(msg=msg)
        try:
            res = Hive.load_queries_from_file(msg_f)
            self.assertEqual(True, res.run().is_ok())
        finally:
            self.delete_local(msg_f)
            self.delete_local(files)

    def test_exist_table_string(self):
        files, msg = self.create_database()
        try:
            res = Hive.load_queries_from_string(msg)
            self.assertEqual(True, res.run().is_ok())
        finally:
            self.delete_local(files)

    def test_exist_database_file(self):
        pass

    def test_change_config_hive(self):
        try:
            hive = Hive.load_queries_from_string("show tables") \
                .with_hive_conf("hive.log.dir", "'/tmp/hivelog'") \
                .use_database("default")
            hive.run()
            import os

            self.assertEqual(os.path.exists("/tmp/hivelog/hive.log"), True)
        finally:
            import shutil

            shutil.rmtree('/tmp/hivelog')

    def create_database(self):
        msg = "create database if not EXISTS testdb; use testdb;"
        msg += "create table some_table(strings STRING)" \
               "ROW FORMAT DELIMITED " \
               "FIELDS TERMINATED BY ','" \
               "STORED AS TEXTFILE;"
        files = self.temp_file(msg="hello,world")
        msg += "LOAD DATA LOCAL INPATH '{0}' OVERWRITE INTO TABLE some_table;".format(files)
        msg += "select * from some_table;"
        msg += "drop table some_table;"
        msg += "drop database testdb;"
        return files, msg

    def temp_file(self, msg=None):
        f = NamedTemporaryFile(mode="w+b", suffix=".hql", delete=False, prefix="hive")
        f.write(msg if msg else "show tables")
        f.close()
        return f.name

    def delete_local(self, path):
        import os

        os.remove(path)

    def delete_file_in_hdfs(self, path="/tmp/hive_table"):
        execute_shell_command("hadoop", "fs", "-rm -R" if path == "/tmp/hive_table" else "-rm", path)

    @classmethod
    def tearDownClass(cls):
        shell.execute_shell_command('hive -e "drop database if EXISTS testdb CASCADE;"')


