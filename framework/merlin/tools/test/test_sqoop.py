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
from unittest2 import TestCase
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore

from merlin.tools.sqoop import Sqoop, SqoopImport


class TestSqoopClient(TestCase):
    def setUp(self):
        super(TestSqoopClient, self).setUp()

    def test_import_table_and_query(self):
        with self.assertRaises(Exception):
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="db").table(table="table_name").query(
                query="ad").to_hdfs().build()

    def test_import_without_database(self):
        with self.assertRaises(Exception):
            Sqoop.import_data().from_rdbms(rdbms="mysql", password_file="/user/cloudera/password", host="localhost",
                                           database="db").to_hdfs().table(table="table_name").build()

    def test_import_without_credential(self):
        with self.assertRaises(Exception):
            Sqoop.import_data().from_rdbms(rdbms="mysql", host="localhost", database="db").to_hdfs().table(
                table="table_name").build()

    def test_import_without_incremental_attributes(self):
        with self.assertRaises(Exception):
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="db").to_hdfs().table(table="table_name"). \
                with_incremental(incremental="append", last_value="12").build()

    def test_import_table(self):
        self.assertEquals(
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").table(
                table="table_name").to_hdfs().build(),
            '--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --as-textfile')

    def test_import_direct(self):
        metastore = IniFileMetaStore(file=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources',
            'sqoop',
            'sqoop.ini'))
        config = Configuration.load(metastore=metastore, readonly=False)
        self.assertEquals(
            SqoopImport.load_preconfigured_job(name="test", config=config).from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                               host="localhost", database="sqoop_tests").with_direct_mode(direct_split_size="1", name_2="12", names_3="1").table(
                                               table="table_name").to_hdfs().build(),
            '-DA=12 -DB=13 --connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --direct -- --name-2=12 --names-3=1')

    def test_import_query(self):
        self.assertEquals(Sqoop.import_data().from_rdbms(rdbms="mysql", username="root",
                                                         password_file="/user/cloudera/sqoop.password",
                                                         host="localhost", database="sqoop_tests").
                          query(query="'SELECT * FROM table_name WHERE $CONDITIONS AND id>$id'", split_by="id",
                                id="2").to_hdfs(target_dir="/custom_directory").build(),
                          "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/sqoop.password --query \"SELECT * FROM table_name WHERE \$CONDITIONS AND id>2\" --split-by id --target-dir /custom_directory --as-textfile")

    def test_import_with_hadoop_properties(self):
        self.assertEquals(
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").
            to_hdfs().table(table="table_name").with_hadoop_properties(some_properties="10").build(),
            "-Dsome.properties=10 --connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --as-textfile")

    def test_import_with_hadoop_properties_from_ini_file(self):
        metastore = IniFileMetaStore(file=os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources',
            'sqoop',
            'sqoop.ini'))
        config = Configuration.load(metastore=metastore, readonly=False)
        self.assertEquals(
            SqoopImport.load_preconfigured_job(name="sqoo", config=config).from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").
            to_hdfs().table(table="table_name").with_hadoop_properties(some_properties="10").build(),
            "-DA=12 -DB=13 -Dsome.properties=10 --connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name")

    def test_import_with_compress(self):
        self.assertEquals(
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").
            to_hdfs().table(table="table_name").with_compress().build(),
            "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --as-textfile --compress")

    def test_import_with_encoding(self):
        self.assertEquals(
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").
            to_hdfs().table(table="table_name").with_encoding(null_string="null", null_non_string="false").build(),
            "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --as-textfile --null-string 'null' --null-non-string 'false'")

    def test_import_with_incremental_attributes(self):
        self.assertEquals(
            Sqoop.import_data().from_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                           host="localhost", database="sqoop_tests").
            to_hdfs().table(table="table_name").with_incremental(incremental="append", last_value="12",
                                                                 check_column="id").build(),
            "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --table table_name --as-textfile --incremental append --check-column id --last-value '12'")

    def test_import_to_hbase(self):
        self.assertEquals(Sqoop.import_data().from_rdbms(rdbms="mysql", username="root",
                                                         password_file="/user/cloudera/rdbms.password",
                                                         host="localhost", database="sqoop_tests"). \
                          table(table="table_name").to_hbase(hbase_table="custom_table",
                                                             hbase_create_table="family", hbase_row_key="id",
                                                             column_family="f1").build(),
                          "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/rdbms.password --table table_name --as-textfile --hbase-table custom_table --hbase-create-table --hbase-row-key id --column-family f1")

    def test_import_to_hive(self):
        self.assertEquals(Sqoop.import_data().from_rdbms(rdbms="mysql", username="root",
                                                         password_file="/user/cloudera/rdbms.password",
                                                         host="localhost", database="sqoop_tests"). \
                          table(table="table_name").to_hive().build(),
                          "--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/rdbms.password --table table_name --as-textfile --hive-import")

    def test_export_without_export_dir(self):
        with self.assertRaises(Exception):
            Sqoop.export_data().to_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                         host="localhost", database="sqoop_tests").table(
                table="table_name").from_hdfs().build()

    def test_export_export_dir(self):
        self.assertEquals(
            Sqoop.export_data().to_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                         host="localhost", database="sqoop_tests").table(table="table_name").from_hdfs(
                export_dir="some").build(),
            '--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --export-dir some --table table_name')

    def test_export_table(self):
        self.assertEquals(
            Sqoop.export_data().to_rdbms(
                rdbms="mysql",
                username="root",
                password_file="/user/cloudera/password",
                host="localhost",
                database="sqoop_tests"
            ).table(table="table_name").from_hdfs(
                export_dir="some"
            ).build(),
            '--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --export-dir some --table table_name')

    def test_export_call(self):
        self.assertEquals(
            Sqoop.export_data().to_rdbms(rdbms="mysql", username="root", password_file="/user/cloudera/password",
                                         host="localhost", database="sqoop_tests").call(
                stored_procedure="procedure").from_hdfs(
                export_dir="some").build(),
            '--connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --export-dir some --call procedure')

    def test_export_batch_with_hadoop_properties(self):
        self.assertEquals(
            Sqoop.export_data().to_rdbms(
                rdbms="mysql",
                username="root",
                password_file="/user/cloudera/password",
                host="localhost",
                database="sqoop_tests"
            ).table(
                table="table_name"
            ).with_hadoop_properties(
                some_properties="10"
            ).with_batch().from_hdfs(
                export_dir="some"
            ).build(),
            '-Dsome.properties=10 --connect jdbc:mysql://localhost/sqoop_tests --username root --password-file /user/cloudera/password --export-dir some --table table_name --batch')





