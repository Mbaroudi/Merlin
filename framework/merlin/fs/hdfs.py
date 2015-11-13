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
import os

from merlin.fs.utils import FileDescriptor
from merlin.common.exceptions \
    import FileSystemException, FileNotFoundException, CommandException
from merlin.common.utils import ListIterator
import merlin.fs.cli.hdfs_commands as fs


class HDFS(object):
    """
    HDFS file manipulation utilities.
    """

    def __init__(self, path, options=None):
        """
        Creates a new HDFS instance
        :param path: path to file
        :param options:
        :type path: str
        :rtype : HDFS
        """
        self.path = path
        self._options = options if options else dict()

    def __iter__(self):
        return ListIterator(self.list_files())

    def __str__(self):
        return self.path

    def __eq__(self, other):
        return self.path == other.path

    def exists(self):
        """
        Tests whether a file exists.
        :rtype : bool
        :return: True if the file exists else False
        """
        return fs.is_file_exists(self.path)

    def is_directory(self):
        """
        Tests whether a file is a directory.
        :rtype : bool
        :return: True if the file is a directory;
        False if the file does not exist, or file is not a directory
        """
        return fs.is_dir(self.path)

    def list_files(self):
        """
        Returns list of the files for the given path
        :rtype : list
        :return: list the files and directories in the current directory or
        list with the single HDFS object when applied to a regular file..
        """
        return [HDFS(path) for path in fs.list_files(self.path)]

    def recursive_list_files(self):
        """
        Returns list of the files for the given path
        :rtype : list
        :return: list the files and directories in the current directory or
        list with the single HDFS object when applied to a regular file..
        """
        return [HDFS(path) for path in fs.recursive_list_files(self.path)]

    def base_dir(self):
        """
        Returns path to base directory of file at the given path
        :rtype HDFS
        """
        return HDFS(os.path.dirname(self.path))

    def create(self, directory=True, recursive=True):
        """
        Creates new empty file or directory
        :param directory: set True if parent directories should also be created
        :param recursive: if True will create directory recursive
        :type directory: bool
        :return:
        """
        return self.create_directory(recursive) if directory else self.create_file(recursive)

    def create_directory(self, recursive=True):
        """
        Creates a new directory unless it already exists

        :param recursive: if True, will create all parent folders
        """
        if not self.exists():
            if recursive and not self.base_dir().exists():
                self.base_dir().create_directory(recursive)
            fs.mkdir(self.path).if_failed_raise(
                FileSystemException(
                    "Cannot create directory '{path}'".format(path=self.path)
                )
            )

    def create_file(self, recursive=True):
        """
        Creates a new empty file unless it already exists

        :param recursive: if True, will create parent folders
        """
        if not self.exists():
            if recursive and not self.base_dir().exists():
                self.base_dir().create_directory(recursive)
            fs.touchz(self.path).if_failed_raise(
                FileSystemException(
                    "Cannot create file '{path}'".format(path=self.path)
                )
            )

    def _assert_exists_(self):
        """
        Tests if file is exists
        :raise: FileNotFoundException when a file with the specified path
        does not exist.
        """
        if not self.exists():
            raise FileNotFoundException(
                "'{path}' does not exists".format(path=self.path)
            )

    def copy_to_local(self, local_path):
        """
        Copies file from HDFS to the local file system
        :param local_path: path to file on local file system
        :type local_path: str
        """
        self._assert_exists_()
        fs.copy_to_local(path=self.path, localdst=local_path).if_failed_raise(
            CommandException(
                "Cannot copy '{path}' to local".format(path=self.path)
            )
        )

    def copy(self, dest):
        """
        Copies files to dest
        :param dest: path to destination file
        :type dest: HDFS
        """
        self._assert_exists_()
        fs.copy(self.path, dest.path).if_failed_raise(
            CommandException(
                "Cannot copy '{path}' to {dst}".format(
                    path=self.path,
                    dst=dest
                )
            )
        )

    def move(self, dest):
        """
        Moves file to dest
        :param dest: path to destination file
        :type dest: str
        """
        self._assert_exists_()
        fs.move(self.path, dest).if_failed_raise(
            CommandException(
                "Cannot copy '{path}' to {dst}".format(
                    path=self.path,
                    dst=dest
                )
            )
        )

    def size(self):
        """
        Calculates aggregate length of files contained in the directory
        or the length of a file in case regular file.
        :return: size of file
        :rtype: long
        """
        self._assert_exists_()
        return fs.file_size(self.path)

    def merge(self, dest_file):
        """
        Merges the files at HDFS path to a single file on the local filesystem
        :param dest_file: path to destination file
        :type dest_file: str
        """
        fs.get_merge(src=self.path, local_dst=dest_file).if_failed_raise(
            CommandException(
                "Cannot merge files from '{path}' to {dst}".format(
                    path=self.path,
                    dst=dest_file
                )
            )
        )

    def delete(self, recursive=False):
        """
        Deletes a file if it exists.
        Directory will be removed only in case
        when recursive flag is set to True
        :param recursive: delete directory recursively
        :type recursive: bool
        """
        if self.exists():
            fs.rm(self.path, recursive).if_failed_raise(
                CommandException(
                    "Cannot delete file '{path}'".format(path=self.path)
                )
            )

    def delete_directory(self):
        """
        Removes directory
        :return:
        """
        return self.delete(recursive=True)

    def permissions(self):
        """
        Gets permission of the file
        :return: permission
        :rtype: str
        """
        return self.get_description().permissions

    def replicas(self):
        """
        Gets replicas of the file
        :return: number of file replicas
        """
        return self.get_description().number_of_replicas

    def owner(self):
        """
        Gets owner of the file
        :return: owner of the file
        :rtype: str
        """
        return self.get_description().owner

    def modification_time(self):
        """
        Gets modification time
        :return: file modification time
        :rtype: str
        """
        return self.get_description().update_date

    def distcp(self, dest, strategy=None, num_mappers=None):
        """
        Copies files between clusters
        :param dest: path to destination directory
        :param strategy: 'update' or 'overwrite'
        :param num_mappers: number of mappers
        :type dest: str
        :type strategy: str
        :param num_mappers: str, int
        """
        fs.distcp(self.path, dest, strategy, num_mappers)

    def get_description(self):
        """
        Gets metadata of file at the given path
        :return: FileDescriptor
        """
        permissions, number_of_replicas, userid, groupid, filesize, modification_date, modification_time, filename = tuple(
            fs.stat(self.path)
        )
        _file_description = FileDescriptor(name=self.path,
                                           update_date=datetime.strptime(
                                               " ".join([modification_date, modification_time[:5]]),
                                               "%Y-%m-%d %H:%M"),
                                           create_date=None,
                                           size=long(filesize),
                                           owner=userid)
        _file_description.number_of_replicas = number_of_replicas
        _file_description.groupid = groupid
        _file_description.permissions = permissions
        return _file_description

    def apply_acl(self, rule):
        """
        Sets ACLs for files and directories.
        :param rule: A comma-separated list of ACL entries.
        """
        return fs.setfacl(self.path, rule)

    def get_acls(self):
        """
        Returns the ACLs of files and directories.
        If a directory has a default ACL, getfacl also displays the default ACL.
        """
        _result = fs.getfacl(self.path)
        return str(_result.stdout).splitlines() if _result.is_ok() else None


