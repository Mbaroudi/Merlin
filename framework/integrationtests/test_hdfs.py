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

from datetime import datetime
import getpass
import os
import socket
import uuid

from unittest2 import skipUnless, TestCase

from merlin.common.exceptions import FileSystemException
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS
from merlin.common.test_utils import has_command
import merlin.common.shell_command_executor as shell


class TestHDFS(TestCase):
    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_exists(self):
        self.assertTrue(HDFS("/tmp").exists())
        self.assertFalse(HDFS("/tmp_12345").exists())

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_is_dir(self):
        self.assertTrue(HDFS("/tmp").is_directory(), "/tmp is not a dir")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_list_files(self):
        basedir = HDFS("/tmp")
        new_file = HDFS("/tmp/test.txt")
        try:
            new_file.create(directory=False)
            self.assertTrue(new_file.exists(), "File was not created")
            files = basedir.list_files()
            self.assertTrue(new_file in files)
        finally:
            new_file.delete()
            self.assertFalse(new_file.exists(), "File was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_recursive_list_files(self):
        basedir = HDFS("/tmp")
        new_folder = HDFS("/tmp/test123")
        new_file = HDFS("/tmp/test123/test.txt")
        try:
            new_folder.create(directory=True)
            self.assertTrue(new_folder.exists(), "Folder was not created")
            new_file.create(directory=False)
            self.assertTrue(new_file.exists(), "File was not created")
            files = basedir.recursive_list_files()
            self.assertTrue(new_file in files)
            self.assertTrue(new_folder in files)
        finally:
            new_folder.delete(recursive=True)
            self.assertFalse(new_file.exists(), "Folder was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_create(self):
        new_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        new_dir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        # tets new file creation
        try:
            new_file.create(directory=False)
            self.assertTrue(new_file.exists(), "File was not created")
            self.assertFalse(new_file.is_directory(), "New file should not be a directory")
        finally:
            new_file.delete()
            self.assertFalse(new_file.exists(), "File was not removed")
            # test new folder creation
        try:
            new_dir.create(directory=True)
            self.assertTrue(new_dir.exists(), "Directory was not created")
            self.assertTrue(new_dir.is_directory(), "New file should be a directory")
        finally:
            new_dir.delete(recursive=True)
            self.assertFalse(new_dir.exists(), "Directory was not removed")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_create_directory(self):
        new_dir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        self.assertFalse(new_dir.exists(), "Directory is already exists")
        try:
            new_dir.create_directory()
            self.assertTrue(new_dir.exists(), "Directory was not created")
            self.assertTrue(new_dir.is_directory())
        finally:
            new_dir.delete(recursive=True)
            self.assertFalse(new_dir.exists(), "Directory was not removed")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_create_file(self):
        new_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        self.assertFalse(new_file.exists(), "File is already exists")
        try:
            new_file.create_file()
            self.assertTrue(new_file.exists(), "File was not created")
            self.assertFalse(new_file.is_directory(), "New file should not be a folder")
        finally:
            new_file.delete()
            self.assertFalse(new_file.exists(), "File was not removed")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def should_create_directory_recursively(self):
        _base_dir = os.path.join('/tmp', str(uuid.uuid4()))
        _path = os.path.join(_base_dir, str(uuid.uuid4()), str(uuid.uuid4()))
        _dir = HDFS(_path)
        self.assertFalse(_dir.exists(), "Folder is already exists")
        try:
            _dir.create_directory(recursive=True)
            self.assertTrue(_dir.exists(), "Folder was not created")
            self.assertTrue(_dir.is_directory(), "New file should be a directory")
        finally:
            HDFS(_base_dir).delete_directory()
            self.assertFalse(_dir.exists(), "File was not removed")
            self.assertFalse(HDFS(_base_dir).exists(), "Base dir was not removed")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def should_create_file_recursively(self):
        _base_dir = os.path.join('/tmp', str(uuid.uuid4()))
        _path = os.path.join(_base_dir, str(uuid.uuid4()), str(uuid.uuid4()), 'file.txt')
        _file = HDFS(_path)
        self.assertFalse(_file.exists(), "File is already exists")
        try:
            _file.create_file(recursive=True)
            self.assertTrue(_file.exists(), "File was not created")
            self.assertFalse(_file.is_directory(), "New file should not be a directory")
        finally:
            HDFS(_base_dir).delete_directory()
            self.assertFalse(_file.exists(), "File was not removed")
            self.assertFalse(HDFS(_base_dir).exists(), "Bse dir was not removed")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def should_raise_error_mkdir_not_recursive(self):
        _base_dir = os.path.join('/tmp', str(uuid.uuid4()))
        _path = os.path.join(_base_dir, str(uuid.uuid4()), str(uuid.uuid4()))
        _dir = HDFS(_path)
        self.assertFalse(_base_dir.exists(), "Folder is already exists")
        try:
            self.assertRaises(FileSystemException, _dir.create_directory, recursive=False)
        finally:
            self.assertFalse(_dir.exists(), "File was created")


    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_copy_to_local(self):
        new_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        local_path = os.path.join("/tmp", "copied_from_hdfs")
        self.assertFalse(os.path.exists(local_path))
        try:
            new_file.create_file()
            self.assertTrue(new_file.exists(), "File was not created")
            new_file.copy_to_local(local_path)
            self.assertTrue(os.path.exists(local_path), "File was not copied from HDFS")
        finally:
            new_file.delete()
            self.assertFalse(new_file.exists(), "File was not removed")
            os.remove(local_path)
            self.assertFalse(os.path.exists(local_path))

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_copy_file(self):
        _file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        dst = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            _file.create_file()
            self.assertTrue(_file.exists(), "original file not found")
            self.assertFalse(dst.exists(), "destination file already exists")
            _file.create()
            _file.copy(dst)
            self.assertTrue(dst.exists(), "file was not copied")
            self.assertTrue(_file.exists(), "original file should not be deleted")
        finally:
            _file.delete()
            dst.delete()
            self.assertFalse(_file.exists(), "File was not deleted")
            self.assertFalse(dst.exists(), "destination file was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_copy_empty_dir(self):
        _dir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        dst = HDFS("/tmp/dst_" + str(uuid.uuid4()))
        try:
            _dir.create(directory=True)
            self.assertTrue(_dir.exists(), "directory not found")
            self.assertFalse(dst.exists(), "dst directory is already exists")
            _dir.copy(dst)
            self.assertTrue(dst.exists(), "directory was not copied")
        finally:
            _dir.delete(True)
            dst.delete(True)
            self.assertFalse(_dir.exists(), "File was not deleted")
            self.assertFalse(dst.exists(), "File was not deleted")

    def _create_non_empty_dir_(self, path):
        _dir = HDFS(path)
        _dir.create_directory()
        self.assertTrue(_dir.exists(), "source directory not found")
        for i in range(5):
            _file = HDFS(os.path.join(path, str(uuid.uuid4())))
            _file.create(directory=(i % 2 == 0))
            self.assertTrue(_file.exists(), "File was not created")
        return _dir

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_copy_non_empty_dir(self):
        dst = HDFS("/tmp/dst_" + str(uuid.uuid4()))
        _dir = None
        try:
            _dir = self._create_non_empty_dir_(os.path.join("/tmp", str(uuid.uuid4())))
            self.assertFalse(dst.exists(), "dst directory is already exists")
            _dir.copy(dst)
            self.assertTrue(dst.exists(), "directory was not copied")
            self.assertTrue(_dir.exists(), "original directory should not be deleted")
        finally:
            if _dir:
                _dir.delete_directory()
                self.assertFalse(_dir.exists(), "Folder was not deleted")
            dst.delete_directory()
            self.assertFalse(dst.exists(), "Dst Folder was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_move_file(self):
        _file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        dst = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            _file.create_file()
            self.assertTrue(_file.exists(), "File was not created")
            self.assertFalse(dst.exists(), "Destination file should not exist")
            _file.move(dst.path)
            self.assertFalse(_file.exists(), "Original file should be deleted")
            self.assertTrue(dst.exists(), "Destination file should be created")
        finally:
            _file.delete()
            dst.delete()
            self.assertFalse(_file.exists(), "File was not deleted")
            self.assertFalse(dst.exists(), "destination file was not deleted")


    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_move_empty_dir(self):
        _dir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        dst = HDFS("/tmp/dst_" + str(uuid.uuid4()))
        try:
            _dir.create(directory=True)
            self.assertTrue(_dir.exists(), "directory not found")
            self.assertFalse(dst.exists(), "destination directory is already exists")
            _dir.move(dst.path)
            self.assertFalse(_dir.exists(), "Original directory was not removed")
            self.assertTrue(dst.exists(), "destination directory was not created")
        finally:
            _dir.delete(True)
            dst.delete(True)
            self.assertFalse(_dir.exists(), "File was not deleted")
            self.assertFalse(dst.exists(), "File was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_move_non_empty_dir(self):
        dst = HDFS("/tmp/dst_" + str(uuid.uuid4()))
        _dir = None
        try:
            _dir = self._create_non_empty_dir_(os.path.join("/tmp", str(uuid.uuid4())))
            self.assertFalse(dst.exists(), "dst directory is already exists")
            _dir.move(dst.path)
            self.assertFalse(_dir.exists(), "original directory should be deleted")
            self.assertTrue(dst.exists(), "directory move operation failed")
        finally:
            if _dir:
                _dir.delete_directory()
                self.assertFalse(_dir.exists(), "Folder was not deleted")
            dst.delete_directory()
            self.assertFalse(dst.exists(), "Dst Folder was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_file_size(self):
        local = LocalFS(os.path.realpath(__file__))
        hdfs_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            local.copy_to_hdfs(hdfs_file.path)
            self.assertTrue(hdfs_file.exists(), "Local file was not copied to HDFS")
            self.assertEqual(hdfs_file.size(), local.size())
        finally:
            hdfs_file.delete()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_dir_size(self):
        local_basedir = os.path.dirname(os.path.realpath(__file__))
        local = LocalFS(os.path.join(local_basedir, "resources", "test_dir_size"))
        hdfs_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            local.copy_to_hdfs(hdfs_file.path)
            self.assertTrue(hdfs_file.exists(), "Local file was not copied to HDFS")
            expected_fsize = local.size()
            self.assertEqual(hdfs_file.size(), expected_fsize)
        finally:
            hdfs_file.delete(recursive=True)

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_merge(self):
        basedir = os.path.dirname(os.path.realpath(__file__))
        local = LocalFS(os.path.join(basedir, "resources", "test_merge"))
        hdfs_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        merged_file = LocalFS(os.path.join(basedir, "resources", "merged.txt"))
        try:
            local.copy_to_hdfs(hdfs_file.path)
            self.assertTrue(hdfs_file.exists(), "Local file was not copied to HDFS")
            hdfs_file.merge(merged_file.path)
            self.assertTrue(merged_file.exists(), "merged file was not copied to local fs")
        finally:
            hdfs_file.delete_directory()
            merged_file.delete()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_delete_file(self):
        _file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        _file.create_file()
        self.assertTrue(_file.exists(), "Target file can not be found")
        _file.delete()
        self.assertFalse(_file.exists(), "Target file was not deleted")


    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_delete_dir(self):
        local = LocalFS(os.path.dirname(os.path.realpath(__file__)))
        hdfs_file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        local.copy_to_hdfs(hdfs_file.path)
        self.assertTrue(hdfs_file.exists(), "Target HDFS dir does not exists")
        hdfs_file.delete(recursive=True)
        self.assertFalse(hdfs_file.exists(), "Target HDFS dir was not deleted")

    # todo FIXIT file permissions can be different in different hadoop versions
    @skipUnless(has_command('hadoop') and True, "Hadoop client should be installed")
    def test_get_permissions(self):
        self.assertEqual("drwxr-xr-x", HDFS("/").permissions(), "Root dir permissions should be 'drwxr-xr-x'")
        # Permissions to '/tmp' folder are different on different CDH versions
        # self.assertEqual("drwxrwxrwt", HDFS("/tmp").permissions(), "Tmp dir permissions should be 'drwxrwxrwxt'")
        hbase_file = HDFS("/hbase/hbase.id")
        if hbase_file.exists():
            self.assertEqual("-rw-r--r--",
                             hbase_file.permissions(),
                             "/hbase/hbase.id permissions should be '-rw-r--r--'")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_get_replicas(self):
        self.assertEqual('0', HDFS("/").replicas(), "Root dir replicas should be 0")
        self.assertNotEqual('0', HDFS("/tmp").replicas(), "dir replicas should be 0")
        name = uuid.uuid4()
        hdfs_file = HDFS("/tmp/{0}".format(name))
        hdfs_file.create_file()
        shell.execute_shell_command('hadoop dfs', '-setrep -w 1 /tmp/{0}'.format(name))
        if hdfs_file.exists():
            self.assertEqual('1',
                             hdfs_file.replicas(),
                             "Number replicas of file must be 1")
            hdfs_file.delete()
            self.assertFalse(hdfs_file.exists())


    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_get_owner(self):
        self.assertEqual('hdfs', HDFS("/").owner(), "ERROR: Root dir owner")
        self.assertEqual('hdfs', HDFS("/tmp").owner(), "ERROR: /tmp dir owner")
        hbase_file = HDFS("/hbase/hbase.id")
        if hbase_file.exists():
            self.assertEqual('hbase', HDFS("/hbase/hbase.id").owner(), "ERROR: /hbase/hbase.id dir owner")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_get_modification_time(self):
        now = datetime.now().strftime("%Y-%m-%d")
        _dir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        _file = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            _dir.create_directory()
            _file.create_file()
            self.assertTrue(_dir.exists(), "Dir was not created")
            self.assertTrue(_file.exists(), "File was not created")
            self.assertEqual(now, _dir.modification_time().strftime("%Y-%m-%d"), "Error: dir modification time")
            self.assertEqual(now, _file.modification_time().strftime("%Y-%m-%d"), "Error: File modification time")
        finally:
            _dir.delete_directory()
            _file.delete()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_distcp(self):
        directory = HDFS("/tmp/bar")
        directory.create()
        new_file = HDFS("/tmp/test_dist.txt")
        new_file.create(directory=False)
        _host = "sandbox.hortonworks.com"
        try:
            self.assertTrue(new_file.exists(), "File was not created")
            _file = HDFS("hdfs://{host}:8020/tmp/test_dist.txt".format(host=_host))
            _file.distcp(dest="hdfs://{host}:8020/tmp/bar/test_dist.txt".format(host=_host))
            file_after_copy = HDFS("/tmp/bar/test_dist.txt")
            self.assertTrue(file_after_copy.exists(), "File was not copied")
        finally:
            new_file.delete()
            directory.delete(recursive=True)
            self.assertFalse(new_file.exists(), "File was not deleted")
            self.assertFalse(directory.delete(), "File was not deleted")

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_get_description(self):
        directory = HDFS("/tmp/bar")
        try:
            directory.create()
            self.assertEqual(directory.get_description().name, "/tmp/bar")
            self.assertEqual(directory.get_description().size, 0)
            self.assertEqual(directory.get_description().owner, getpass.getuser())
            self.assertEqual(directory.get_description().create_date, None)

        finally:
            directory.delete(recursive=True)
            self.assertFalse(directory.delete(), "File was not deleted")











