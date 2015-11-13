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
Bootstrap script can be used to create initial directory structure
and apply necessary ACL rules

Usage:
    python bootstrap.py <path to config file>
HF should be installed before running this script

Configuration file structure
    - [acls] section describes data access model and contains access level to ACLs rules mapping
    - [hdfs] section  describes HDFS directory structure. Specific ACL rules can be specified for each directory
    - [local fs] section describes local file system directory structure.

Configuration example:
    [acls]
    pii=group:sys-pii:r-x
    sales=group:sales:r--

    [files]
    /raw/sales=pii,sales
    /tmp/raw/tmp=

    [local fs]
    /tmp/data_tmp=

"""
import sys

from merlin.common.configurations import Configuration
from merlin.common.logger import get_logger
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS

# ACL
CONFIG_ACLS_KEY = 'acls'
CONFIG_HDFS_DIRS_KEY = 'hdfs'
CONFIG_LOCAL_FS_DIRS_KEY = 'local fs'


class FsSnapshot(object):
    def __init__(self):
        super(FsSnapshot, self).__init__()
        self.logger = get_logger(self.__class__.__name__)
        self.files = {}

    @staticmethod
    def load_from_config(config, fs_section, acl_section=None):
        """
        Loads FsSnapshot from configuration. see merlin.common.configurations.Configuration
        :param config: configuration
        :param fs_section: name of the config section, which contains directory structure description
        :param acl_section: name of the config section, which describes data access model
        :return: FsSnapshot
        """
        fs = FsSnapshot()
        for path in config.options(fs_section):
            fs.add_file(path)
            if acl_section:
                fs.attach_acls(path=path,
                               acls=[config.get(acl_section, group) for group in
                                     config.get(fs_section, path).split(',')])
        return fs

    def add_file(self, path):
        """Adds specific folder to FSSnapshot"""
        if path not in self.files:
            self.files[path] = []

    def attach_acls(self, path, acls):
        """Adds ACL for specific file"""
        self.add_file(path)
        self.files[path].extend(acls)

    def apply(self,
              mkdir_command=lambda path: True,
              apply_acls_command=lambda path, acls: True):
        """
        creates initial directory structure and applies required ACL rules
        :param mkdir_command:  lambda function, which takes path as argument and can be used to create directory
        :param apply_acls_command: lambda function, which takes path and ACLs as arguments
            and can be used to apply specific ACL rules
        """
        for file, acls in self.files.iteritems():
            mkdir_command(file)
            if acls:
                self.__apply_acls__(file, acls, apply_acls_command)

    def __apply_acls__(self, path, acls, apply_acls_command):
        if acls:
            for acl in acls:
                if acl:
                    apply_acls_command(path, acl)

    def __str__(self):
        return '\n'.join("{!s} : {!r}".format(key, val) for (key, val) in self.files.items())


def apply_hdfs_snapshot(config):
    """Creates initial directory structure on HDFS and applies ACL rules """
    _hdfs_snapshot = FsSnapshot.load_from_config(config,
                                                 fs_section=CONFIG_HDFS_DIRS_KEY,
                                                 acl_section=CONFIG_ACLS_KEY)
    _hdfs_snapshot.apply(
        mkdir_command=lambda path: HDFS(path).create_directory(recursive=True),
        apply_acls_command=lambda path, acls: HDFS(path).apply_acl(acls)
    )


def apply_localfs_snapshot(config):
    """Creates initial directory structure on local file system"""
    _localfs_snapshot = FsSnapshot.load_from_config(
        config,
        fs_section=CONFIG_LOCAL_FS_DIRS_KEY
    )
    _localfs_snapshot.apply(
        mkdir_command=lambda path: LocalFS(path).create_directory()
    )


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Usage: python bootstrap.py <config file>'
        sys.exit(-1)
    _config_file = sys.argv[1]
    _configs = Configuration.load(_config_file)
    apply_hdfs_snapshot(_configs)
    apply_localfs_snapshot(_configs)