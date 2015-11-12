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

"""
Util for file system

Example of manipulation:

Create left - list of FileDescriptor that contains next values in field 'name' :
["file001.txt", "file002.txt", "file003.txt", "file004.txt"]

Create right - list of FileDescriptor that contains next values in field name' :
["file002.txt"]

FileUtils.get_new_files(left, right) - returns list of FileDescriptor that
contains next value in field 'name' :
"file001.txt", "file003.txt", "file004.txt"]

FileDescriptor has data as:
    -size
    -name
    -owner
    -update date
    -create date

"""

from merlin.common.utils import ListUtility


class FileUtils(object):
    """
    Util for monitoring new files
    """

    @staticmethod
    def name_extractor(path):
        """
        Gets name's attribute from the given path (FileDescriptor)
        :param path: descriptor that contains of metadata of file
        :type path: FileDescriptor
        :rtype:
        """
        if not hasattr(path, 'name'):
            raise TypeError('FileDescriptor is required. '
                            'Cannot extract file name from {0}'.format(path.__class__))
        return path.name

    @staticmethod
    def get_new_files(left,
                      right,
                      left_property_extractor=None,
                      right_property_extractor=None):
        """
        Returns list files that exist in 'left' list and don't exist in 'right' list
        :type left: list
        :type right: list
        :rtype: list
        """
        return ListUtility.diff(left,
                                right,
                                left_property_extractor if left_property_extractor else FileUtils.name_extractor,
                                right_property_extractor if right_property_extractor else FileUtils.name_extractor)


class FileDescriptor(object):
    """
    FileDescriptor is class that contains of metadata of file.
    It is common descriptor for files on ftp, hdfs or local file systems.
    FileDescriptor has data as:
    -size
    -name
    -owner
    -update date
    -create date
    """

    def __init__(self,
                 name=None,
                 update_date=None,
                 create_date=None,
                 size=None,
                 owner=None):
        self.name = name
        self.update_date = update_date
        self.create_date = create_date
        self.size = size
        self.owner = owner

    def __str__(self):
        return "name:'{name}', update_date:{update_date}, create_date:{create_date}, " \
               "size:{size}B, owner:{owner}".format(name=self.name, update_date=self.update_date,
                                                    create_date=self.create_date,
                                                    size=self.size,
                                                    owner=self.owner)

    def __eq__(self, other):
        return self.name == other.name

    def __getattr__(self, name):
        return self.__dict__[name] if name in self.__dict__ else None