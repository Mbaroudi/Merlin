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

from unittest2 import TestCase

from bootstrap.bootstrap import FsSnapshot, CONFIG_ACLS_KEY, CONFIG_HDFS_DIRS_KEY
from merlin.common.configurations import Configuration


class FsSnapshotTest(TestCase):
    def test_fsimage_from_config(self):
        config = Configuration.create(readonly=False, accepts_nulls=True)
        config.set(section=CONFIG_ACLS_KEY,
                   key='confidential',
                   value='user:su:rwx')
        config.set(section=CONFIG_ACLS_KEY,
                   key='sales',
                   value='group:sales:r-x')
        config.set(section=CONFIG_HDFS_DIRS_KEY,
                   key='/raw/sales',
                   value='confidential,sales')
        snapshot = FsSnapshot.load_from_config(config=config,
                                               fs_section=CONFIG_HDFS_DIRS_KEY,
                                               acl_section=CONFIG_ACLS_KEY)
        files = snapshot.files

        self.assertTrue('/raw/sales' in files,
                        'File was not added to fs snapshot')
        self.assertTrue('user:su:rwx' in files['/raw/sales'],
                        '\'confidential\' access lvl was not mapped to file')
        self.assertTrue('group:sales:r-x' in files['/raw/sales'],
                        '\'sales\' access lvl was not mapped to file')
        self.assertFalse('default:fake:r-x' in files['/raw/sales'],
                         'Error in access lvl mapping')

    def test_fsimage_from_config_withot_acls(self):
        config = Configuration.create(readonly=False, accepts_nulls=True)
        config.set(section=CONFIG_HDFS_DIRS_KEY,
                   key='/raw/sales',
                   value=None)
        snapshot = FsSnapshot.load_from_config(config=config,
                                               fs_section=CONFIG_HDFS_DIRS_KEY)
        files = snapshot.files

        self.assertTrue('/raw/sales' in files,
                        'File was not added to fs snapshot')
        self.assertTrue(len(files['/raw/sales']) == 0,
                        'ACL should be ignored for current configuration')