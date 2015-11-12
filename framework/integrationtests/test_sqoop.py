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
import getpass
import socket

from unittest2 import TestCase, skipUnless
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore

import merlin.common.shell_command_executor as shell
from merlin.fs.hdfs import HDFS
from merlin.tools.sqoop import Sqoop, SqoopExport, SqoopImport
from merlin.common.test_utils import has_command

"""
To run this test make sure:
 1. MySql Driver is installed within the system
 2. mysql client is available via command line
 3. MySQl user root with password root should be created
 4. If 1,2,3 conditions are met enable RUN_TEST flag
 5. Mysqldump utility should be installed on individual node machines. To run direct related test do enable MYSQLDUMP flag
 6. HBASE service should be installed and running. To run Hbase related tests do enable HBASE_IS_RUNNING flag
 7. HIVE service should be installed and running. To run Hive related tests do enable HIVE_IS_RUNNING flag
"""


MYSQLDUMP = True
MYSQL_SERVER = "sandbox.hortonworks.com"
BASE_DIR = "/tmp"
RUN_TEST = True
HBASE_IS_RUNNING = True
HIVE_IS_RUNNING = True
ZOOKEEPER_IS_RUNNING = False
USER = "root"

# Must be equals to text in resources/rdbms.password
PASSWORD = "root"


@skipUnless(has_command('sqoop') and has_command('mysql') and RUN_TEST,
            "sqoop and mysql clients should be installed and mysql must have user 'root' with password 'root'. Also we must add to /var/lib/sqoop jar with jdbc driver")
class TestSqoop(TestCase):
    @classmethod
    def setUpClass(cls):
        shell.execute_shell_command('hadoop fs', '-rm -r {0}/rdbms.password'.format(BASE_DIR))
        shell.execute_shell_command('hadoop fs', '-copyFromLocal',
                                    os.path.join(os.path.dirname(__file__),
                                                 'resources/sqoop/rdbms.password'),
                                    BASE_DIR)
        shell.execute_shell_command('hadoop fs', '-mkdir', os.path.join(BASE_DIR, "data_custom_directory"))
        shell.execute_shell_command('hadoop fs', '-copyFromLocal',
                                    os.path.join(os.path.dirname(__file__),
                                                 'resources/sqoop/data_to_export.txt'),
                                    os.path.join(BASE_DIR, "data_custom_directory"))
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'DROP DATABASE IF EXISTS sqoop_tests'")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'CREATE DATABASE sqoop_tests'")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'CREATE TABLE IF NOT EXISTS table_name(id INT(11) NOT NULL AUTO_INCREMENT,"
            "last_name varchar(255) NOT NULL, first_name varchar(255), city varchar(255),"
            "PRIMARY KEY (id))'")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'CREATE TABLE IF NOT EXISTS table_name_second(id INT(11) NOT NULL AUTO_INCREMENT,"
            "last_name varchar(255) NOT NULL, first_name varchar(255), city varchar(255),"
            "PRIMARY KEY (id))'")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'CREATE TABLE IF NOT EXISTS stag(id INT(11) NOT NULL AUTO_INCREMENT,"
            "last_name varchar(255) NOT NULL, first_name varchar(255), city varchar(255),"
            "PRIMARY KEY (id))'")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "\"INSERT INTO table_name (last_name) VALUES ('Bob')\"")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "\"INSERT INTO table_name (last_name, first_name, city) VALUES ('Alex','Log','New York')\"")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "\"INSERT INTO table_name (last_name, first_name, city) VALUES ('Merry','Log','New York')\"")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "\"INSERT INTO table_name (last_name, first_name, city) VALUES ('Bob','Log','New York')\"")
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "\"delimiter //\ncreate procedure p(in p_id INT, in p_last_name varchar(255), "
            "in p_first_name varchar(255), in p_city varchar(255)) begin insert into table_name_second("
            "id, last_name, first_name, city) values(p_id,p_last_name,p_first_name,p_city);\nend//\"")

    def test_import_table(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopImport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).from_rdbms().table(
                table="table_name", where="id>2",
                columns="id,last_name").to_hdfs(
                target_dir="{0}/custom_directory".format(BASE_DIR)).run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_export_table(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    @skipUnless(MYSQLDUMP, "mysqldump utility should be installed on individual node machines")
    def test_export_table_with_direct_mode(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).with_direct_mode().run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    def test_export_table_with_batch(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).with_batch().with_hadoop_properties(
                sqoop_export_records_per_statement="10").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    def test_export_table_with_encoding(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).with_encoding(input_null_string="NNN").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    def test_export_table_with_staging(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).with_staging_table(staging_table="stag").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM stag'")

    def test_export_table_with_call(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).call(stored_procedure="p").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    def test_export_table_with_update(self):
        try:
            metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__),
                                                                   'resources/sqoop/custom.ini'))
            cmd = SqoopExport.load_preconfigured_job(
                config=Configuration.load(metastore=metastore,
                                           readonly=False,
                                           accepts_nulls=True)).to_rdbms().table(
                table="table_name_second").from_hdfs(
                export_dir="{0}/data_custom_directory".format(BASE_DIR)).with_update(update_key="id",
                                                                                     update_mode="allowinsert").run()
            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'SELECT * FROM table_name_second'")
            self.assertNotEqual(result.stdout.split(' ')[0], 'Empty', result.stdout)
        finally:
            shell.execute_shell_command(
                'mysql --user {0} --password={1} --host={2} sqoop_tests -e'.format(USER, PASSWORD, MYSQL_SERVER),
                "'DELETE FROM table_name_second'")

    def test_import_query(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").query(
                query="'SELECT * FROM table_name WHERE $CONDITIONS AND id>$id'", split_by="id", id="2").to_hdfs(
                target_dir="{0}/custom_directory".format(BASE_DIR)).run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_to_sequencefile(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).use_file_format(
                file_format="--as-sequencefile").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_to_avrodatafile(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).use_file_format(
                file_format="--as-avrodatafile").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs',
                                                 '-du -s {0}/custom_directory/part-m-*.avro'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    @skipUnless(MYSQLDUMP, "mysqldump utility should be installed on individual node machines")
    def test_import_with_direct_mode(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(
                target_dir="{0}/custom_directory".format(BASE_DIR)).with_direct_mode().run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_with_compress(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).with_compress(
                compression_codec="org.apache.hadoop.io.compress.BZip2Codec").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs',
                                                 '-du -s {0}/custom_directory/part-m-*.bz2'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_with_incremental(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).with_incremental(
                incremental="append", last_value="5", check_column="id").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_with_null(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).with_encoding(
                null_string="N").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_with_connection_manager(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).with_attr(
                connection_manager="org.apache.sqoop.manager.MySQLManager").run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    def test_import_with_enclosing(self):
        try:
            cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                                 password_file="{0}/rdbms.password".format(BASE_DIR),
                                                 database="sqoop_tests").table(
                table="table_name").to_hdfs(target_dir="{0}/custom_directory".format(BASE_DIR)).with_input_parsing(
                escaped_by="\\").with_output_parsing(escaped_by="\\", mysql_delimiters=True).run()

            self.assertEquals(cmd.status, 0, cmd.stderr)
            result = shell.execute_shell_command('hadoop fs', '-du -s {0}/custom_directory/part-m-*'.format(BASE_DIR))
            self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:
            shell.execute_shell_command('hadoop fs', '-rm -r {0}/custom_directory'.format(BASE_DIR))

    @skipUnless(has_command('hbase') and HBASE_IS_RUNNING and ZOOKEEPER_IS_RUNNING, "hbase client should be installed")
    def test_import_to_hbase(self):
        cmd = Sqoop.import_data().from_rdbms(host=MYSQL_SERVER, rdbms="mysql", username="root",
                                             password_file="{0}/rdbms.password".format(BASE_DIR),
                                             database="sqoop_tests").table(table="table_name").to_hbase(
            hbase_table="custom_table", hbase_create_table=True, hbase_row_key="id", column_family="f1").run()

        self.assertEquals(cmd.status, 0, cmd.stderr)
        # HDP Cluster has another path to HBase data
        # result = shell.execute_shell_command('hadoop fs', '-du -s /user/hive/warehouse/table_name/part-m-*')
        # self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)

    @skipUnless(has_command('hive') and HIVE_IS_RUNNING, "hive client should be installed")
    def test_import_to_hive(self):
        _path = HDFS(os.path.join('/user', getpass.getuser(), 'table_name'))
        try:
            if _path.exists():
                _path.delete(recursive=_path.is_directory())
                # shell.execute_shell_command('hadoop fs', '-rm -r /user/', getpass.getuser(), '/table_name')
            cmd = Sqoop.import_data().from_rdbms(
                host=MYSQL_SERVER,
                rdbms="mysql",
                username="root",
                password_file="{0}/rdbms.password".format(BASE_DIR),
                database="sqoop_tests"
            ).table(
                table="table_name"
            ).to_hive().run()

            # self.assertEquals(cmd.status, 0, cmd.stderr)
            # result = shell.execute_shell_command('hadoop fs', '-du -s /user/hive/warehouse/table_name/part-m-*')
            # self.assertNotEqual(result.stdout.split(' ')[0], '0', result.stdout)
        finally:

            shell.execute_shell_command('hive', "-e 'DROP TABLE IF EXISTS table_name'")

    @classmethod
    def tearDownClass(cls):
        shell.execute_shell_command('hadoop fs', '-rm -r {0}/rdbms.password'.format(BASE_DIR))
        shell.execute_shell_command('hadoop fs', '-rm -r {0}/data_custom_directory'.format(BASE_DIR))
        shell.execute_shell_command(
            'mysql --user {0} --password={1} --host={2} -e'.format(USER, PASSWORD, MYSQL_SERVER),
            "'DROP DATABASE IF EXISTS sqoop_tests'")
