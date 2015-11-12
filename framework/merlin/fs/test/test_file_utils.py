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
from unittest2.util import unorderable_list_difference

from merlin.fs.utils import FileUtils, FileDescriptor


class TestFileUtils(TestCase):
    def test_get_new_files(self):
        self.assertGetNewFiles(
            left=[],
            right=[],
            expected_seq=[],
            scenario_name="empty lists"
        )

        self.assertGetNewFiles(
            left=["file001.txt"],
            right=[],
            expected_seq=["file001.txt"],
            scenario_name="new file"
        )

        self.assertGetNewFiles(
            left=[],
            right=["file001.txt"],
            expected_seq=[],
            scenario_name="without new files"
        )

        self.assertGetNewFiles(
            left=["file001.txt", "file002.txt", "file003.txt", "file004.txt"],
            right=["file002.txt"],
            expected_seq=["file001.txt", "file003.txt", "file004.txt"],
            scenario_name="few new files"
        )

    def assertGetNewFiles(self, left, right, expected_seq, scenario_name):
        expected = sorted([FileDescriptor(name=_file) for _file in expected_seq])
        actual = sorted(FileUtils.get_new_files(
            left=[FileDescriptor(name=_file) for _file in left],
            right=[FileDescriptor(name=_file) for _file in right]))
        missing, unexpected = unorderable_list_difference(expected, actual)
        self.assertTrue(len(missing) == 0, "%s : Missing values %s " % (scenario_name, " ".join(missing)))
        self.assertTrue(len(unexpected) == 0, "%s : Unexpected values %s " % (scenario_name, " ".join(unexpected)))



