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

from mock import patch, Mock
from unittest2 import TestCase, expectedFailure

from merlin.common.shell_command_executor import build_command, Result
import merlin.fs.cli.hdfs_commands as hdfs_client


HDFS_IS_DIR_FUNC = 'merlin.fs.cli.hdfs_commands.is_dir'


class TestHdfsClient(TestCase):
    def setUp(self):
        super(TestHdfsClient, self).setUp()

    def _assert_command_generation(self, expected_command, status=0, stdout=None, stderr=None):
        def executor(command, *args):
            self.assertEqual(build_command(command, *args), expected_command)
            return Mock(spec=Result, status=status, stdout=stdout, stderr=stderr)

        return executor

    @classmethod
    def tearDownClass(cls):
        super(TestHdfsClient, cls).tearDownClass()

    def test_mkdir_command_generator(self):
        hdfs_client.mkdir(path="/tmp/test",
                          executor=lambda command, *args: self._assert_command_generation("hadoop fs -mkdir /tmp/test")(
                              command, *args))

    @expectedFailure
    def test_mkdir_command_generator_negative(self):
        hdfs_client.mkdir("/tmp/test",
                          executor=lambda command, *args: self.assertEqual(
                              build_command(command, *args),
                              "mkdir /tmp/test"))

    def test_copy_to_local_command_generator(self):
        hdfs_client.copy_to_local(path="/tmp/test", localdst="~/dir",
                                  executor=lambda command, *args: self.assertEqual(
                                      build_command(command, *args),
                                      "hadoop fs -copyToLocal /tmp/test ~/dir"))

    def test_copy_from_local_command_generator(self):
        hdfs_client.copy_from_local(localsrc="~/data.txt",
                                    hdfsdst="/tmp/dir",
                                    executor=lambda command, *args: self.assertEqual(
                                        build_command(command, *args),
                                        "hadoop fs -copyFromLocal ~/data.txt /tmp/dir"))

    def test_copy_command_generator(self):
        hdfs_client.copy(files="/tmp/data.txt",
                         dest="/raw/dir",
                         executor=lambda command, *args: self.assertEqual(
                             build_command(command, *args),
                             "hadoop fs -cp /tmp/data.txt /raw/dir"
                         ))

        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = True
            hdfs_client.copy(files=["/tmp/file_001.txt", "/tmp/file_002.txt"],
                             dest="/raw/dir",
                             executor=lambda command, *args: self.assertEqual(
                                 build_command(command, *args),
                                 "hadoop fs -cp /tmp/file_001.txt /tmp/file_002.txt /raw/dir"
                             ))

    def test_move_command_generator(self):
        hdfs_client.move(files="/tmp/data.txt",
                         dest="/raw/dir",
                         executor=lambda cmd, *args: self._assert_command_generation(
                             "hadoop fs -mv /tmp/data.txt /raw/dir")(cmd, *args))

        hdfs_client.move(files=["/tmp/file_001.txt", "/tmp/file_002.txt"],
                         dest="/raw/dir",
                         executor=lambda cmd, *args: self._assert_command_generation(
                             "hadoop fs -mv /tmp/file_001.txt /tmp/file_002.txt /raw/dir")(cmd, *args))

    def test_is_file_exists_command_generator(self):
        hdfs_client.is_file_exists("/tmp/data.txt", executor=lambda cmd, *args: self._assert_command_generation(
            "hadoop fs -test -e /tmp/data.txt")(cmd, *args))

    def test_is_file_not_exists_command(self):
        # shell_client.EXECUTOR = self._executor_to_fail
        self.assertFalse(
            hdfs_client.is_file_exists("/tmp/data.txt",
                                       executor=lambda cmd, *args: self._assert_command_generation(
                                           "hadoop fs -test -e /tmp/data.txt", status=-1)(cmd, *args)))

    def test_is_dir_command_generator(self):
        hdfs_client.is_dir("/tmp/data",
                           executor=lambda cmd, *args: self._assert_command_generation("hadoop fs -test -d /tmp/data")(
                               cmd, *args))

    def test_is_not_a_dir_command(self):
        self.assertFalse(hdfs_client.is_dir("/tmp/data",
                                            executor=lambda cmd, *args: self._assert_command_generation(
                                                "hadoop fs -test -d /tmp/data", status=-1)(cmd, *args)))

    def test_list_files_command_generator(self):
        _stdout = ("Found 5 items\n"
                   "drwxr-xr-x   - hbase supergroup          0 2014-09-18 23:38 /hbase\n"
                   "drwxr-xr-x   - solr  solr                0 2014-07-31 10:38 /solr\n"
                   "drwxrwxrwx   - hdfs  supergroup          0 2014-09-26 04:34 /tmp\n"
                   "drwxr-xr-x   - hdfs  supergroup          0 2014-09-11 03:47 /user\n"
                   "drwxr-xr-x   - hdfs  supergroup          0 2014-07-31 10:37 /var\n")
        files = hdfs_client.list_files("/",
                                       executor=lambda cmd, *args:
                                       self._assert_command_generation("hadoop fs -ls /", stdout=_stdout)(cmd, *args))

        self.assertListEqual(files, ["/hbase", "/solr", "/tmp", "/user", "/var"])

    @expectedFailure
    def test_list_files_command_generator_negative(self):
        _stdout = ("Found 5 items\n"
                   "drwxr-xr-x   - hbase supergroup          0 2014-09-18 23:38 /hbase\n"
                   "drwxr-xr-x   - solr  solr                0 2014-07-31 10:38 /solr\n"
                   "drwxrwxrwx   - hdfs  supergroup          0 2014-09-26 04:34 /tmp\n"
                   "drwxr-xr-x   - hdfs  supergroup          0 2014-09-11 03:47 /user\n"
                   "drwxr-xr-x   - hdfs  supergroup          0 2014-07-31 10:37 /var\n")
        files = hdfs_client.list_files("/",
                                       executor=lambda cmd, *args:
                                       self._assert_command_generation("hadoop fs -ls /", stdout=_stdout)(cmd, *args))

        self.assertListEqual(files, ["/hbase"])

    def test_file_size_command(self):
        _stdout = "3137674753  /tmp/file.txt"
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = False

            size = hdfs_client.file_size("/tmp/file.txt",
                                         executor=lambda cmd, *args: self._assert_command_generation(
                                             "hadoop fs -du /tmp/file.txt",
                                             stdout=_stdout)(cmd, *args))
            self.assertEqual(3137674753, size)

    def test_directory_size_command(self):
        _stdout = "3137674753  /tmp/file.txt"
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = True
            size = hdfs_client.file_size("/tmp/file.txt",
                                         executor=lambda cmd, *args: self._assert_command_generation(
                                             "hadoop fs -du -s /tmp/file.txt", stdout=_stdout)(cmd, *args))
            self.assertEqual(3137674753, size)

    def test_get_merge_command(self):
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = True
            hdfs_client.get_merge(src="/user/test/data",
                                  local_dst="~/data.txt",
                                  executor=lambda cmd, *args: self._assert_command_generation(
                                      "hadoop fs -getmerge /user/test/data ~/data.txt")(cmd, *args))

    def test_touchz(self):
        hdfs_client.touchz(path="/user/test/data.txt",
                           executor=lambda cmd, *args: self._assert_command_generation(
                               "hadoop fs -touchz /user/test/data.txt")(cmd, *args))

    def test_remove_file(self):
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = False
            hdfs_client.rm(path="/user/test/data.txt",
                           executor=lambda cmd, *args: self._assert_command_generation(
                               "hadoop fs -rm /user/test/data.txt"))

    def test_remove_dir(self):
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = True
            hdfs_client.rm(path="/user/test/data",
                           executor=lambda cmd, *args: self._assert_command_generation(
                               "hadoop fs -rm /user/test/data")(cmd, *args))
            #     recursive flag should be set explicitly
            hdfs_client.rm(path="/user/test/data",
                           recursive=True,
                           executor=lambda cmd, *args: self._assert_command_generation(
                               "hadoop fs -rm -R /user/test/data")(cmd, *args))

    def test_stat_dir(self):
        _stdout = ("Found 4 items\n"
                   "drwxr-xr-x   - hbase hbase               0 2014-10-01 08:50 /hbase\n"
                   "drwxrwxr-x   - solr  solr                0 2014-10-01 08:51 /solr\n"
                   "drwxrwxrwt   - hdfs  supergroup          0 2014-10-03 14:13 /tmp\n"
                   "drwxr-xr-x   - hdfs  supergroup          0 2014-10-01 09:11 /user")
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = True
            stat = hdfs_client.stat("/user",
                                    executor=lambda cmd, *args: self._assert_command_generation(
                                        os.path.join("hadoop fs -ls /user", ".."),
                                        stdout=_stdout)(cmd, *args))
            self.assertListEqual(["drwxr-xr-x", "-", "hdfs", "supergroup", "0", "2014-10-01", "09:11", "/user"],
                                 stat)

    def test_stat_file(self):
        _stdout = ("Found 1 items\n"
                   "-rwxr-xr-x   3 vagrant vagrant   65652975 2014-10-01 09:12 /user/vagrant/dmode.txt")
        with patch(HDFS_IS_DIR_FUNC) as mock_isdir:
            mock_isdir.return_value = False
            stat = hdfs_client.stat("/user/vagrant/dmode.txt",
                                    executor=lambda cmd, *args: self._assert_command_generation(
                                        "hadoop fs -ls /user/vagrant/dmode.txt",
                                        stdout=_stdout
                                    )(cmd, *args))
            self.assertListEqual(
                ["-rwxr-xr-x", "3", "vagrant", "vagrant", "65652975", "2014-10-01", "09:12",
                 "/user/vagrant/dmode.txt"],
                stat)

    def test_setfacl(self):
        hdfs_client.setfacl("/tmp/test", "user:hadoop:rw-",
                            executor=lambda command, *args: self.assertEqual(
                                build_command(command, *args),
                                "hadoop fs -setfacl -m user:hadoop:rw- /tmp/test"))

    def test_getfacl(self):
        hdfs_client.getfacl("/tmp/test",
                            executor=lambda command, *args: self.assertEqual(
                                build_command(command, *args),
                                "hadoop fs -getfacl /tmp/test"))





