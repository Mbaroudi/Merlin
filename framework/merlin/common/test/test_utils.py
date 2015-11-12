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

from merlin.common.utils import ListUtility
from merlin.fs.utils import FileDescriptor
from unittest2 import TestCase


class TestUtils(TestCase):

    def setUp(self):
        super(TestUtils, self).setUp()
        self.left = ["file001.txt", "file002.txt", "file003.txt", "file004.txt"]
        self.right = ["file002.txt"]
        self.left_extractor = None
        self.right_extractor = None

    def test_diff(self):
        list_ = ListUtility.diff(left=self.left, right=self.right)
        self.assertTrue("file001.txt" in list_)
        self.assertTrue("file003.txt" in list_)
        self.assertFalse("file002.txt" in list_)

    def test_intersect(self):
        list_ = ListUtility.intersect(left=self.left, right=self.right)
        self.assertFalse("file001.txt" in list_)
        self.assertFalse("file003.txt" in list_)
        self.assertTrue("file002.txt" in list_)

    def test_to_string(self):
        self.assertEquals(ListUtility.to_string(self.right), "file002.txt")

    def test_to_dict(self):
        dictionary = ListUtility.to_dict(self.left)
        self.assertTrue(('file002.txt', 'file002.txt') in dictionary.iteritems())
        self.assertFalse(('file003.txt', 'file002.txt') in dictionary.iteritems())
        list_ = [FileDescriptor(name="file001"), FileDescriptor(name="file002")]
        dictionary = ListUtility.to_dict(list_, key_extractor=FileDescriptor().name)
        self.assertTrue((FileDescriptor(name="file001"), FileDescriptor(name="file001")) in dictionary.iteritems())
        self.assertFalse((FileDescriptor(name="file001"), FileDescriptor(name="file002")) in dictionary.iteritems())
