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
import shutil
import uuid

from unittest2 import TestCase, expectedFailure, skipUnless

import merlin.common.shell_command_executor as shell
from merlin.fs.ftp import sftp_client, FTPClient


USER = "user"
PASSWORD = "user"
HOST = "10.253.132.220"
HKEY_PATH = os.path.join(os.path.dirname(__file__), 'resources/ftp/known_hosts')
FLAG = True
BASE_DIR = uuid.uuid4()


@skipUnless(FLAG, "Config")
class TestFTPConnector(TestCase):
    def setUp(self):
        super(TestFTPConnector, self).setUp()

        ftp = sftp_client(HOST, path="{0}/folder/".format(BASE_DIR), login=USER, password=PASSWORD, hkey_path=HKEY_PATH)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=True, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertTrue(ftp.is_directory())

        ftp = sftp_client(HOST, path="{0}/file/".format(BASE_DIR), login=USER, password=PASSWORD, hkey_path=HKEY_PATH)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=False, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertFalse(ftp.is_directory())

        ftp = sftp_client(HOST, path="{0}/folder2".format(BASE_DIR), login=USER, password=PASSWORD, hkey_path=HKEY_PATH)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=True, create_parents=True)
        self.assertTrue(ftp.exists())
        self.assertTrue(ftp.is_directory())

        ftp = sftp_client(HOST, path="{0}/folder/file".format(BASE_DIR), login=USER, password=PASSWORD, hkey_path=HKEY_PATH)
        self.assertFalse(ftp.exists())
        if not ftp.exists():
            ftp.create(make_dir=False, create_parents=False)
        self.assertTrue(ftp.exists())
        self.assertFalse(ftp.is_directory())

    def test_is_exists(self):
        ftp = sftp_client(HOST, "{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.exists())
        ftp = sftp_client(HOST, "{0}/file/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.exists())
        ftp = sftp_client(HOST, "{0}/folder/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.exists())
        ftp = sftp_client(HOST, "{0}/{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertFalse(ftp.exists())
        ftp = sftp_client(HOST, "{0}/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.exists())

    def test_basedir_root(self):
        ftp = sftp_client(HOST, "/", USER, PASSWORD, HKEY_PATH)
        self.assertEqual(str(ftp.base_dir().get_description().name), '/')

    def test_basedir(self):
        ftp = sftp_client(HOST, "{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertEqual(str(ftp.base_dir().get_description().name), '/home/user')

    def test_size(self):
        ftp = sftp_client(HOST, "{0}/folder".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertEqual(ftp.size(), 0)
        ftp = sftp_client(HOST, "{0}/file".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertEqual(ftp.size(), 0)

    def test_list_files(self):
        ls = []
        for ftp in sftp_client(HOST, "{0}/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH):
            ls.append(ftp.get_description().name)

        self.assertEqual(str(ls), "[u'{0}/file', u'{0}/folder2', u'{0}/folder']".format(BASE_DIR))

    def test_is_dir(self):
        ftp = sftp_client(HOST, "{0}/folder".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.is_directory())
        ftp = sftp_client(HOST, "{0}/file".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertFalse(ftp.is_directory())

    def test_upload(self):
        ftp = sftp_client(HOST, "{0}/folder".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        ftp.upload(local_path=os.path.join(os.path.dirname(__file__), 'resources/ftp/zero'))
        ftp = sftp_client(HOST, "{0}/folder/zero".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertTrue(ftp.exists())
        ftp = sftp_client(HOST, "{0}/file".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        ftp.upload(local_path=os.path.join(os.path.dirname(__file__), 'resources/ftp/zero'), update=True)
        self.assertTrue(ftp.exists())

    def test_download_file(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            ftp = sftp_client(HOST, "{0}/file/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_file(local_path=os.path.join(os.path.dirname(__file__), 'resources/download/'))
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/file')))

            ftp.download_file(local_path=os.path.join(os.path.dirname(__file__), 'resources/download'))
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/file')))
            shell.execute_shell_command('rm', os.path.join(os.path.dirname(__file__), 'resources/download/file'))

            ftp.download_file(local_path=os.path.join(os.path.dirname(__file__), 'resources/download/file/'))
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/file')))
        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    @expectedFailure
    def test_download_file_invalid_path(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            ftp = sftp_client(HOST, "{0}/file".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_file(local_path=os.path.join(os.path.dirname(__file__), 'resources/download/zero/file'))
            self.assertFalse(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/zero/file')))
        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    def test_download_dir(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            ftp = sftp_client(HOST, "{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_dir(local_path=os.path.join(os.path.dirname(__file__), 'resources/download'))
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}'.format(BASE_DIR))))
            self.assertTrue(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/file'.format(BASE_DIR))))
            self.assertTrue(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/folder'.format(BASE_DIR))))
            self.assertTrue(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/folder/file'.format(BASE_DIR))))
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download/{0}'.format(BASE_DIR)))

            ftp.download_dir(local_path=os.path.join(os.path.dirname(__file__), 'resources/download'), recursive=False)
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}'.format(BASE_DIR))))
            self.assertTrue(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/file'.format(BASE_DIR))))
            self.assertFalse(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/folder'.format(BASE_DIR))))
            self.assertFalse(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/folder/file'.format(BASE_DIR))))
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download/{0}'.format(BASE_DIR)))

        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    def test_download_dir_with_predicate(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            ftp = sftp_client(HOST, "{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_dir(local_path=os.path.join(os.path.dirname(__file__), 'resources/download'),
                             predicate=self.predicate_get_description, recursive=True)
            self.assertTrue(os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}'.format(BASE_DIR))))
            self.assertTrue(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/file'.format(BASE_DIR))))
            self.assertFalse(
                os.path.exists(os.path.join(os.path.dirname(__file__), 'resources/download/{0}/folder'.format(BASE_DIR))))

        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    def predicate_get_description(self, path, connector):
        ftp = FTPClient(path, connector)
        print path
        if not ftp.is_directory():
            return True
        return False

    def test_get_description(self):
        ftp = sftp_client(HOST, "{0}/folder2".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        self.assertEqual(ftp.get_description().name, "{0}/folder2".format(BASE_DIR))
        self.assertEqual(ftp.get_description().create_date, None)
        self.assertEqual(ftp.get_description().owner, None)
        self.assertEqual(ftp.get_description().size, 0)

    @expectedFailure
    def test_download_dir_invalid_path_sftp(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            ftp = sftp_client(HOST, "{0}/file".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_dir(local_path=os.path.join(os.path.dirname(__file__), 'resources/download'))

        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    @expectedFailure
    def test_download_dir_invalid_path_local(self):
        try:
            shell.execute_shell_command('mkdir', os.path.join(os.path.dirname(__file__), 'resources/download'))
            shell.execute_shell_command('touch', os.path.join(os.path.dirname(__file__), 'resources/download/zero'))
            ftp = sftp_client(HOST, "{0}/".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
            self.assertTrue(ftp.exists())

            ftp.download_dir(local_path=os.path.join(os.path.dirname(__file__), 'resources/download/zero'))

        finally:
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'resources/download'))

    def tearDown(self):
        ftp = sftp_client(HOST, "{0}".format(BASE_DIR), USER, PASSWORD, HKEY_PATH)
        ftp.delete(recursive=True)
        self.assertFalse(ftp.exists())
