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
from unittest2.case import skipUnless

from bootstrap.bootstrap import apply_hdfs_snapshot, apply_localfs_snapshot
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS


DFS_NAMENODE_ACLS_ENABLED = True


class FsSnapshotTest(TestCase):
    @skipUnless(DFS_NAMENODE_ACLS_ENABLED, "Support for ACLs has been disabled by setting dfs.namenode.acls.enabled to false")
    def test_apply_hdfs_snapshot(self):
        _config_file = os.path.join(os.path.dirname(__file__),
                                    'resources',
                                    'bootsrap',
                                    'bootstrap.ini')
        _raw_sales_dir = HDFS('/tmp/raw/sales')
        _raw_users_dir = HDFS('/tmp/raw/users')
        _raw_tmp_dir = HDFS('/tmp/raw/tmp')
        try:
            # run bootstrap script
            metastore = IniFileMetaStore(file=_config_file)
            _config = Configuration.load(metastore)
            apply_hdfs_snapshot(_config)

            # asserts
            # assert directories were created
            self.assertTrue(_raw_sales_dir.exists(), "Directory '/tmp/raw/sales' was not created")
            self.assertTrue(_raw_users_dir.exists(), "Directory '/tmp/raw/users' was not created")
            self.assertTrue(_raw_tmp_dir.exists(), "Directory '/tmp/raw/tmp' was not created")
            # assert acls were applied
            sales_dir_acls = _raw_sales_dir.get_acls()
            users_dir_acls = _raw_users_dir.get_acls()

            self.assertIsNotNone(sales_dir_acls, '/tmp/raw/sales : ACL were not applied')
            self.assertTrue('group:sys-pii:r-x' in sales_dir_acls, '/tmp/raw/sales : pii acl was not applied')
            self.assertTrue('group:sales:r--' in sales_dir_acls, '/tmp/raw/sales : salse acl was not applied')

            self.assertIsNotNone(users_dir_acls, '/tmp/raw/users : ACL were not applied')
            self.assertTrue('group:sys-pii:r-x' in sales_dir_acls, '/tmp/raw/users : pii acl was not applied')
        finally:
            _test_basedir = HDFS('/tmp/raw')
            _test_basedir.delete_directory()
            self.assertFalse(_test_basedir.exists(), "ERROR: clean up failed")

    def test_apply_local_fs_snapshot(self):
        _config_file = os.path.join(os.path.dirname(__file__),
                                    'resources',
                                    'bootsrap',
                                    'bootstrap.ini')
        test_dir = LocalFS('/tmp/data_tmp')
        if test_dir.exists():
            test_dir.delete_directory()
        try:
            metastore = IniFileMetaStore(file=_config_file)
            _config = Configuration.load(metastore)
            apply_localfs_snapshot(_config)
            self.assertTrue(test_dir.exists(), "Folder was not created")
        finally:
            test_dir.delete_directory()


