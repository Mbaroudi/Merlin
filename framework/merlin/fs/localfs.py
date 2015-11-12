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

from dircache import listdir
import datetime
import os
import shutil

from merlin.fs.utils import FileDescriptor
from merlin.common.exceptions import FileNotFoundException, CommandException
from merlin.common.utils import ListIterator
import merlin.fs.cli.hdfs_commands as hdfs


class LocalFS(object):
    """
    Local file manipulation utilities.
    """

    def __init__(self, path):
        super(LocalFS, self).__init__()
        self.path = path

    def __iter__(self):
        return ListIterator(self.list_files())

    def __str__(self):
        return "file {0}".format(self.path)

    def exists(self):
        """
        Tests whether a file exists.

        :rtype : bool
        :return: true if the file exists; false if the file does not exist
        """
        return os.path.exists(self.path)

    def is_directory(self):
        """
        Tests whether a file is a directory.

        :rtype : bool
        :return: true if the file is a directory;
        false if the file does not exist, or file is not a directory
        """
        return os.path.isdir(self.path)

    def list_files(self):
        """
        List the file for the given path

        :rtype : list
        :return: list the files and directories in the current directory or
         list with the single HDFS object when applied to a regular file..
        """
        return [LocalFS(path) for path in listdir(self.path)]

    def create(self, directory=True):
        """
        Creates new empty file or directory
        :param directory: set True if directory should be created
        :return:
        """
        return self.create_directory() if directory else self.create_file()

    def create_directory(self):
        """
        Creates a new directory unless it already exists.

        """
        if not self.exists():
            try:
                os.makedirs(self.path)
            except OSError:
                if not self.is_directory():
                    raise

    def create_file(self):
        """
        Creates a new and empty file unless it already exists.

        """
        if not self.exists():
            open(self.path, 'a').close()

    def assert_exists(self):
        """
        Tests if file is exists.

        :raise: FileNotFoundException when a file with the specified path
        does not exist.
        """
        if not self.exists():
            raise FileNotFoundException(
                "'{path}' does not exists".format(path=self.path)
            )

    def assert_is_dir(self):
        """
        Tests if file is directory.

        :raise: FileNotFoundException when a file with the specified path
        is not directory.
        """
        if not self.is_directory():
            raise FileNotFoundException(
                "'{path}' is not directory".format(path=self.path)
            )

    def copy_to_hdfs(self, hdfs_path):
        """
        Copies file from local file system to HDFS.

        :param file to copy:
        """
        self.assert_exists()
        hdfs.copy_from_local(
            localsrc=self.path,
            hdfsdst=hdfs_path
        ).if_failed_raise(
            CommandException(
                "Cannot copy '{path}' to HDFS".format(path=self.path)
            )
        )

    def size(self):
        """
        Calculate aggregate length of files contained in the directory
        or the length of a file in case regular file.

        :return: file size in bytes
        """
        return self.dir_size() if self.is_directory() \
            else os.path.getsize(self.path)

    def dir_size(self):
        """
         Calculate  length of the directory

        :return: aggregated length of files contained in the directory
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.path):
            for _file in filenames:
                _absolute_path = os.path.join(dirpath, _file)
                total_size += os.path.getsize(_absolute_path)
        return total_size

    def modification_time(self):
        """Return the last modification time of a file, reported by os.stat()."""
        _ts = os.path.getmtime(self.path)
        return datetime.datetime.fromtimestamp(_ts)

    def creation_time(self):
        """Return the metadata creation time of a file, reported by os.stat()."""
        _ts = os.path.getctime(self.path)
        return datetime.datetime.fromtimestamp(_ts)

    def owner(self):
        """Return user ID of the file's owner. """
        return os.stat(self.path).st_uid

    def delete(self):
        """
         Remove a file

        """
        os.remove(self.path)

    def delete_directory(self):
        """Recursively delete a directory tree."""
        shutil.rmtree(self.path)

    def get_description(self):
        """
        Get metadata of file at the given path
        :return: FSDescription
        """
        return FileDescriptor(name=self.path,
                              update_date=self.modification_time(),
                              create_date=self.creation_time(),
                              size=self.size(),
                              owner=self.owner())

    def copy(self, dest, copystat=False):
        """
        Copies files to dest
        :param copystat: Copy all stat info (mode bits, atime, mtime, flags) from src to dst
        :param dest: path to destination file
        :type dest:
        """
        self.assert_exists()
        shutil.copy(self.path, dest)
        if copystat:
            shutil.copystat(self.path, dest)

    def move(self, dest):
        """
        Recursively move a file or directory to another location.
        :param dest: path to destination file
        :type dest: str
        :return pointer to a new file
        :rtype LocalFS
        :raises FileNotFoundException in case src file does not exist
        """
        self.assert_exists()
        shutil.move(self.path, dest)
        return LocalFS(dest)

    def permissions(self):
        """
        Gets permission of the file
        :return: permission
        :rtype: str
        """
        return os.stat(self.path).st_mode





