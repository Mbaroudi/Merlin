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
import re
import tempfile
from ConfigParser import RawConfigParser
import uuid

from unittest2.case import TestCase
from unittest2.main import main

from merlin.fs.ftp import ftp_client, FTPClient
from merlin.common.exceptions import FileNotFoundException

BASE_DIR = uuid.uuid4()


class TestFTPConnector(TestCase):
    def setUp(self):
        super(TestFTPConnector, self).setUp()

        config = RawConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), "resources/ftp/ftp_config.ini"))
        self.host_download = config.get("ftp", "host.download")
        self.user_name = config.get("ftp", "user.name")
        self.password = config.get("ftp", "password")
        self.path = config.get("ftp", "path")

        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=True, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertTrue(ftp.is_directory())

        ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR), self.user_name, self.password)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=False, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertFalse(ftp.is_directory())

        ftp = ftp_client(self.host_download, "{0}/folder/file".format(BASE_DIR), self.user_name, self.password)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=False, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertFalse(ftp.is_directory())

    def test_is_exist(self):
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        self.assertTrue(ftp.exists(), "not correct root")
        ftp = ftp_client(self.host_download, "{0}/folder/s".format(BASE_DIR), self.user_name, self.password)
        self.assertFalse(ftp.exists(), "correct path")
        ftp = ftp_client(self.host_download, "asd/qwe", self.user_name, self.password)
        self.assertFalse(ftp.exists(), "correct path")

    def test_size(self):
        ftp = ftp_client(self.host_download, "/", self.user_name, self.password)
        self.assertEqual(0, ftp.size(), "Folder has size")
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        self.assertEqual(0, ftp.size(), "Folder has size")
        ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR), self.user_name, self.password)
        self.assertEqual(0, ftp.size(), "File has size")
        ftp = ftp_client(self.host_download, "{0}/folder/file".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertEqual(0, ftp.size(), "not equal size")

    def test_list_files(self):
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        self.assertEqual(2, len(ftp.list_files()), "bad count")
        self.assertEqual(list, ftp.list_files().__class__, "list files has return bad type")
        ftp = ftp_client(self.host_download, "{0}/asd".format(BASE_DIR), self.user_name, self.password)
        self.assertRaises(FileNotFoundException, ftp.list_files)

    def test_upload(self):
        ftp = ftp_client(self.host_download, "{0}/file2".format(BASE_DIR), self.user_name, self.password)
        ftp.upload(os.path.join(os.path.dirname(__file__), "resources/ftp/ftp_config.ini"))
        ftp = ftp_client(self.host_download, "{0}/file2".format(BASE_DIR), self.user_name, self.password)
        ftp.upload(os.path.join(os.path.dirname(__file__), "resources/ftp/ftp_config.ini"), update=True)
        ftp = ftp_client(self.host_download, "{0}/file2".format(BASE_DIR), self.user_name, self.password)
        self.assertTrue(ftp.exists())
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        ftp.upload(os.path.join(os.path.dirname(__file__), "resources/ftp/ftp_config.ini"))
        ftp = ftp_client(self.host_download, "{0}/ftp_config.ini".format(BASE_DIR), self.user_name, self.password)
        self.assertTrue(ftp.exists())

    def test_download_file(self):
        tmp = tempfile.gettempdir()
        path = os.path.join(tmp, "file")
        try:
            ftp = ftp_client(self.host_download, "{0}/".format(BASE_DIR), self.user_name, self.password)
            self.assertRaises(Exception, ftp.download_file, tmp)
            ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR),
                             self.user_name, self.password)
            ftp.download_file(tmp)
            self.assertTrue(os.path.exists(path))
        finally:
            os.remove(path)

    def predicate(self, path, connector):
        ftp = FTPClient(path, connector)
        name = ftp.get_description().name
        if re.search("^c\w+", name):
            return True
        return False

    def test_download_dir(self):
        tmp = tempfile.gettempdir()
        path = os.path.join(tmp, "{0}".format(BASE_DIR))
        try:
            ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR), self.user_name, self.password)
            self.assertRaises(Exception, ftp.download_file(tmp))
            ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR),
                             self.user_name, self.password)
            ftp.download_dir(tmp, recursive=True)
            self.assertTrue(os.path.exists(path))
        finally:
            import shutil

            shutil.rmtree(path)

    def test_download_dir_with_predicate(self):
        tmp = tempfile.gettempdir()
        path = os.path.join(tmp, "{0}".format(BASE_DIR))
        try:
            ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR),
                             self.user_name, self.password)
            ftp.download_dir(tmp, self.predicate, recursive=True)
            self.assertTrue(os.path.exists(path))
        finally:
            import shutil

            shutil.rmtree(path)

    def test_basedir(self):
        ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertEqual("{0}".format(BASE_DIR), ftp.base_dir().get_description().name)
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertEqual("", ftp.base_dir().get_description().name)

    def test_is_dir(self):
        ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertFalse(ftp.is_directory(), "Path is dir")
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertTrue(ftp.is_directory(), "Path is file")

    def test_get_description(self):
        ftp = ftp_client(self.host_download, "{0}/file".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertEqual(ftp.get_description().name, "{0}/file".format(BASE_DIR))
        self.assertNotEqual(ftp.get_description().update_date, None)
        self.assertEqual(ftp.get_description().owner, None)
        self.assertEqual(ftp.get_description().size, 0)
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR),
                         self.user_name, self.password)
        self.assertEqual(ftp.get_description().update_date, None)

    def tearDown(self):
        ftp = ftp_client(self.host_download, "{0}".format(BASE_DIR), self.user_name, self.password)
        ftp.delete(recursive=True)
        self.assertFalse(ftp.exists())

if __name__ == '__main__':
    main()