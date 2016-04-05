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
FTP API


SFTPConnector is wrapper for paramiko.sftp_client.SFTP
FTPConnector, FTPSConnector are wrappers for ftplib


Creates and returns FTPClient with SFTPConnector:
    ftp = sftp_client(host='localhost', path='/tmp/folder/file', login='user',
                      password='$$$$', hkey_path='/known_hosts')

Creates and returns FTPClient with FTPConnector:
    ftp = ftp_client(host='localhost', path='/tmp/folder/file', login='user',
                     password='$$$$', port=21, passive=True)

Creates and returns FTPClient with FTPSConnector:
    ftp = ftp_client(host='localhost', path='/tmp/folder/file', login='user',
                     password='$$$$', port=21, auth=False, protocol=True)


FTPClient has next methods for work with different ftp servers:
    Checks if file at the path is exists
        ftp.is_exists()

    Checks if file at the path is directory
        ftp.is_dir()

    Returns size from file at the path.
    Returns 0 for folder
        ftp.size()

    Returns a list FTPClient containing the names of the entries in the path
        ftp.list_files()

    Creates file at the path.
        ftp.create()

    Creates directory if parameter 'make_dir' is True.
    Moreover creates all parent`s directory for directory
        ftp.create(make_dir=True)

    Creates file at the path.
    Also creates all parent`s directory for file if 'create_parents' is True
        ftp.create(create_parents=True)

    Removes the files or empty directory at the path
        ftp.remove()

    Deletes directory with files if parameter 'recursive' is True
        ftp.remove(recursive=True)

    Copies a local file '/tmp/file' to the SFTP server as path.
    Updates exists file if parameter 'update' is True
        ftp.upload('/tmp/file', update=True)

    Copies a remote file (path) from the SFTP server to the local host as local_path.
    Local path can be existing file that will be update or existing directory for file
        ftp.download_file('/tmp/file')
        ftp.download_file('/tmp/folder')

    Copies a remote directory (path) with files from the SFTP server
    to the local host into a local_path. In addition, copies inner directories
    if parameter 'recursive' is True. Filters file if predicate was given
        ftp.download_dir('/tmp/folder', predicate=only_directories, recursive=True)
        def only_directories(self, path, connector):
            ftp = FTPClient(path, connector)
            if ftp.is_dir():
                return True
            return False

    Gets FTPClient with path on base directory
        ftp.base_dir()

    Gets metadata of file at the path
        ftp.get_description()

    Closes the FTP session and its underlying channel
        ftp.close()

"""

import os
import re
import StringIO
from stat import S_ISDIR
from ftplib import FTP, FTP_TLS, error_perm
from datetime import datetime

from merlin.fs.utils import FileDescriptor
from merlin.common.exceptions import FileNotFoundException, FTPFileError, FTPConnectorError, FTPPredicateError
from merlin.fs.localfs import LocalFS


def sftp_client(host, path=None, login=None, password=None, hkey_path=None):
    """
    Creates and returns FTPClient with path to file and SFTP connection
    :param host: host of SFTP server
    :param path: path to file or dictionary on SFTP server
    :param login: user's name
    :param password: password for user
    :param hkey_path: path to ssh key
    :return: FTPClient with SFTP connection
    :type host: str
    :type path: str
    :type login: str
    :type password: str
    :type hkey_path: str
    :rtype: FTPClient
    """
    return get_ftp_client(path, SFTPConnector.get_session_sftp(host, login, password, hkey_path))


def ftp_client(host, path=None, login=None, password=None, port=None, passive=True):
    """
    Creates and returns FTPClient with path to file and FTP connection
    :param host: host of FTP server
    :param path: path to file or dictionary on FTP server
    :param login: user's name
    :param password: password for user
    :param port: port of FTP server
    :param passive: enable passive mode of FTP server if True (default), or switch to Active mode if False
    :return: FTPClient with FTP connection
    :type host: str
    :type path: str
    :type login: str
    :type password: str
    :type port: int
    :rtype: FTPClient
    """

    return get_ftp_client(path,
                          FTPConnector.
                          get_session_ftp(host, login, password, port, passive))


def ftps_client(path, host, login=None, password=None, port=21, auth=False, protocol=True):
    """
    Creates and returns FTPClient with path to file and FTPS connection
    :param host: host of FTPS server
    :param path: path to file or dictionary on FTPS server
    :param login: user's name
    :param password: password for user
    :param port: port of FTPS server
    :param auth: if it is true sets up secure control
    connection by using TLS/SSL
    :param protocol: if it is true sets up secure data connection
    else sets up clear text data connection
    :return: FTPClient with FTPS connection
    :type host: str
    :type path: str
    :type login: str
    :type password: str
    :type port: int
    :type auth: bool
    :type protocol: bool
    :rtype: FTPClient
    """
    return get_ftp_client(path,
                          FTPSConnector.
                          get_session_ftps(host, login, password, port, auth, protocol))


def get_ftp_client(path, ftp_connector):
    """
    Returns FTPClient with path to file and FTP/SFTP/FTPS connection
    :param path: path to file
    :param ftp_connector: connector for specific ftp server
    :type path: str
    :type ftp_connector: FTPSClient, FTPClientWrapper, SFTPClient
    :rtype: FTPClient
    """
    return FTPClient(path, ftp_connector)


class FTPClient(object):
    """
    Interface for communicating with an FTP/FTPS/SFTP servers.
    """

    def __init__(self, path, ftp_connector):
        """

        :type ftp_connector: FTPSClient, FTPClientWrapper, SFTPClient
        """
        self.ftp_connector = ftp_connector
        self.path = path

    def __iter__(self):
        """
        :rtype: FTPClient
        """
        return iter(self.list_files())

    def __enter__(self, ):
        """
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        :param exc_type:
        :param exc_val:
        :param exc_tb:
        :return:
        """
        self.close()

    def exists(self):
        """
        Checks if file at the path is exists
        :rtype : bool
        """
        return self.ftp_connector.exists(self.path)

    def is_directory(self):
        """
        Checks if file at the path is directory
        :rtype bool
        """
        return self.ftp_connector.is_directory(self.path)

    def modification_time(self):
        """
        Returns last modification time
        :rtype: str
        """
        return self.ftp_connector.modification_time(self.path)

    def size(self):
        """
        Returns size from file at the path.
        Returns 0 for folder
        :rtype: long
        """
        return self.ftp_connector.size(self.path)

    def list_files(self):
        """
        Returns a list FTPClient containing the names of the entries
        in the given path
        :rtype: list
        """
        return [FTPClient(i, self.ftp_connector)
                for i in self.ftp_connector.list_files(self.path)]

    def create(self, make_dir=False, create_parents=False):
        """
        Creates file at the path.
        Creates directory if parameter 'make_dir' is True.
        Creates all parent`s directory if 'create_parents' is True
        :param make_dir: creates directory if this param is True
        :param create_parents: creates all parents directory if is True
        :return: FTPClient
        :type make_dir: bool
        :type create_parents: bool
        :rtype FTPClient
        """
        return self.ftp_connector.create(self.path, make_dir, create_parents)

    def delete(self, recursive=False):
        """
        Removes the files or empty directory at the path.
        Deletes directory with files if parameter 'recursive' is True
        :param recursive: deletes directory with all files if is True
        :type recursive: bool
        """
        self.ftp_connector.delete(self.path, recursive)

    def upload(self, local_path, update=False):
        """
        Copies a local file (local_path) to the SFTP server as path.
        Updates exists file if parameter 'update' is True
        :param local_path: path to file on local file system
        :param update: updates file on ftp if is True
        :type local_path: str
        :type update: bool
        """
        self.ftp_connector.upload(self.path, local_path, update)

    def download_file(self, local_path):
        """
        Copies a remote file (path) from the SFTP server
        to the local host as local_path
        :param local_path: path to future file or existing directory
        :type local_path: str
        """
        self.ftp_connector.download_file(self.path, local_path)

    def download_dir(self, local_path, predicate=lambda path, connector: True, recursive=True):
        """
        Copies a remote directory (path) with files from the SFTP server
        to the local host into a local_path. In addition, copies inner directories
        if parameter 'recursive' is True. Filters file if predicate was given
        :param local_path: path to directory on local file system
        :param predicate: predicate for filters file
        :param recursive: copies all inner directory at the given path if is True
        :type local_path: str
        :type recursive: bool
        """
        return self.ftp_connector.download_dir(self.path, local_path, predicate, recursive)

    def base_dir(self):
        """
        Gets FTPClient with path on base directory
        :return: FTPClient
        """
        return FTPClient(self.ftp_connector.base_dir(self.path), self.ftp_connector)

    def get_description(self):
        """
        Gets metadata of file at the path
        :return: FileDescription
        """
        return self.ftp_connector.get_description(self.path)

    def close(self):
        """
        Closes the FTP session and its underlying channel
        """
        self.ftp_connector.close()


class FTPConnector(object):
    """
    Connector for ftp server
    """

    @staticmethod
    def get_session_ftp(host, login=None, password=None, port=None, passive=True):
        """
        :param host:
        :param login:
        :param password:
        :param passive: enable passive mode of FTP server if True (default), or switch to Active mode if False
        :raise: error_perm
        :return:
        """
        ftp = FTP()
        try:
            ftp.set_pasv(passive)
            ftp.connect(host, port)
            ftp.login(login, password)
            return FTPConnector(ftp)
        except error_perm, exp:
            raise FTPConnectorError(
                exp.message
            )

    def __init__(self, ftp_driver):
        self.__ftp_driver = ftp_driver

    def __assert_exists(self, path):
        """
        Checks if file is exists
        :param path: path to file or directory
        :type path: str
        :raise FileNotFoundException:
        """
        if not self.exists(path):
            raise FileNotFoundException(
                "'{path}' does not exists".format(path=path)
            )

    def list_files(self, path):
        """
        Returns a list containing the names of the entries
        in the given path
        :param path: path to file or directory
        :type path: str
        :rtype list, str
        """
        self.__assert_exists(path)
        return self.__ftp_driver.nlst(path)

    def size(self, path):
        """
        Returns size from file at the given path.
        Returns 0 for folder
        :param path: path to file or directory
        :type path: str
        :rtype: long
        """
        self.__assert_exists(path)
        return 0 if self.is_directory(path) else self.__ftp_driver.size(path)

    def exists(self, path):
        """
        Checks if file at the given path is exists
        :param path: path to file or directory
        :type path: str
        :rtype : bool
        """
        base_path = self.base_dir(path)
        tmp = self.__ftp_driver.nlst(base_path)
        return True if path in tmp else self.__is_root(path)

    def __is_root(self, path):
        """
        Checks if path is root
        :param path: path to file
        :rtype: bool
        """
        return True if path == '/' or path == '' else False

    def is_directory(self, path):
        """
        Checks if file at the given path is directory
        :param path: path to file or directory
        :type path: str
        :rtype bool
        """
        self.__assert_exists(path)
        try:
            self.__ftp_driver.cwd(path)
        except error_perm:
            return False
        else:
            self.__ftp_driver.cwd("/")
            return True

    def __create_local_dir(self, path, local_path):
        """
        Creates local directory
        :param path: path to file on ftp
        :param local_path: path to file on local file system
        :return:
        """
        LocalFS(local_path).assert_exists()
        names = os.path.basename(path)
        local_path = os.path.join(local_path, names)
        LocalFS(local_path).create_directory()
        return local_path

    def download_dir(self, path, local_path, predicate=lambda path, connector: True, recursive=True):
        """
        Copies a remote directory (path) with files from the SFTP server
        to the local host into a local_path. In addition, copies inner directories
        if parameter 'recursive' is True. Filters file if predicate was given
        :param path: path to directory on ftp
        :param local_path: path to directory on local file system
        :param predicate: predicate for filter file
        :param recursive: copies all inner directory at the given path if is True
        :type path: str
        :type local_path: str
        :type recursive: bool
        """
        self.__assert_exists(path)
        local_path = self.__create_local_dir(path, local_path)
        list_path = [path for path in self.list_files(path) if predicate(path, self)]
        for path in list_path:
            if self.is_directory(path) and recursive:
                self.download_dir(path, local_path, predicate, recursive)
            elif not self.is_directory(path):
                self.download_file(path, local_path)
        return self

    def __assert_is_not_dir(self, path):
        """
        Checks if file is directory
        :param path: path to file or directory
        :type path: str
        :raise FTPFileError
        """
        if self.is_directory(path):
            raise FTPFileError(
                "{0} is folder".format(self.get_description(path).name)
            )

    def download_file(self, path, local_path):
        """
        Copies a remote file (path) from the SFTP server
        to the local host as local_path
        :param path: path to file on ftp
        :param local_path: path to future file or existing directory
        :type path: str
        :type local_path: str
        """
        self.__assert_exists(path)
        LocalFS(local_path).assert_exists()
        self.__assert_is_not_dir(path)
        self.__ftp_driver.retrbinary("RETR {0}".format(path),
                                     open(os.path.join(local_path,
                                                       os.path.basename(path)),
                                          "w+b").write)

    def __copy_file_from_local(self, local_path, path, create_parents=False):
        """
        Copies file from local
        """
        LocalFS(local_path).assert_exists()
        base_dir = self.base_dir(path)
        if self.exists(base_dir):
            self.__ftp_driver.storbinary("STOR {0}".format(path), open(local_path, "rb"))
        else:
            self.__assert_recursive(create_parents)
            self.create_dir(base_dir)
            self.__ftp_driver.storbinary("STOR {0}".format(path), open(local_path, "rb"))

    def create_file(self, path, create_parents=False):
        """
        Creates file at the given path if it doesn't exist.
        Creates all parent`s directory if create_parents is True
        :param path: path to file
        :param create_parents: creates all parents directory if is True
        :type path: str
        :type create_parents: bool
        :rtype FTPConnector
        """
        base_dir = self.base_dir(path)
        if self.exists(base_dir):
            self.__ftp_driver.storbinary("STOR {0}".format(path), StringIO.StringIO())
        else:
            self.__assert_recursive(create_parents)
            self.create_dir(base_dir)
            self.__ftp_driver.storbinary("STOR {0}".format(path), StringIO.StringIO())
        return self

    def upload(self, path, local_path, update=False):
        """
        Copies a local file (local_path) to the SFTP server as path.
        Updates exists file if parameter 'update' is True
        :param path: path to file on ftp
        :param local_path: path to file on local file system
        :param update: updates file on ftp if is True
        :type path: str
        :type local_path: str
        :type update: bool
        """
        base_path = self.base_dir(path)
        LocalFS(local_path).assert_exists()
        if self.exists(path):
            if self.is_directory(path):
                name_files = os.path.basename(local_path)
                self.__copy_file_from_local(local_path, "/".join([path, name_files]))
            else:
                self.__assert_is_update(update)
                self.__copy_file_from_local(local_path, path)
        else:
            self.__assert_exists(base_path)
            name_files = path[len(base_path):]
            self.__copy_file_from_local(local_path, "/".join([base_path, name_files]))

    def __assert_is_update(self, update):
        """
        Checks if file is not exists
        :type update: bool
        :raise: FTPFileError
        """
        if not update:
            raise FTPFileError(
                "File is exist"
            )

    def create_dir(self, path):
        """
        Creates directory at the given path if it doesn't exist.
        Creates all parent`s directory
        :param path: path to directory
        :type path: str
        :rtype FTPConnector
        """
        base_dir = self.base_dir(path)
        if not self.exists(base_dir):
            self.create_dir(base_dir)
        self.__ftp_driver.mkd(path)
        return self

    def create(self, path, make_dir=False, create_parents=False):
        """
        Creates file at the given path.
        Creates directory if parameter 'make_dir' is True.
        Creates all parent`s directory if create_parents is True
        :param path: path to file or directory
        :param make_dir: creates directory if this param is True
        :param create_parents: creates all parents directory if is True
        :return: FTPConnector
        :type path: str
        :type make_dir: bool
        :type create_parents: bool
        :rtype FTPConnector
        """
        return self.create_dir(path) \
            if make_dir else \
            self.create_file(path, create_parents)

    def __assert_recursive(self, recursive):
        """
        Checks if file is not exists
        :type recursive: bool
        :raise FTPPredicateError:
        """
        if not recursive:
            raise FTPPredicateError(
                "Parent directory doesn't exists"
            )

    def delete(self, path, recursive=False):
        """
        Removes the files or empty directory at the given path.
        Deletes directory with files if parameter 'recursive' is True
        :param path: path to file or directory
        :param recursive: deletes directory with all files if is True
        :type path: str
        :type recursive: bool
        """
        self.__assert_exists(path)
        list_f = self.list_files(path)
        if self.is_directory(path):
            if recursive:
                for path_ftp in list_f:
                    self.delete(path_ftp, recursive)
            self.__ftp_driver.rmd(path)
        else:
            self.__ftp_driver.delete(path)

    def base_dir(self, path):
        """
        Gets path to base directory of file at the given path
        :param path: path to file
        :return: path to base directory
        :type path: str
        :rtype: str
        """
        return os.path.dirname(path)

    def get_description(self, path):
        """
        Gets metadata of file at the given path
        This method uses standart MDTM command to get file's last modification date.
        According to RFC 3659, MDTM is only specified for files, which may be retrieved,
        and can not be applied to a directory.
        Thus we return None as the directory's modification time
        :param path: path to file or directory
        :return: FileDescriptor with metadata of file at the given path
        :type path: str
        :rtype: FileDescriptor
        """
        tmp_list_status = []
        if path != "":
            self.__assert_exists(path)
            self.__ftp_driver.dir(self.base_dir(path), tmp_list_status.append)
            res = self.__build_obj_description(path, tmp_list_status)
        else:
            res = FileDescriptor(name="",
                                 size=0)
        return res

    def modification_time(self, path):
        """
        Returns last modification time
        This method uses standart MDTM command to get file's last modification date.
        According to RFC 3659, MDTM is only specified for files, which may be retrieved,
        and can not be applied to a directory.
        Thus we return None as the directory's modification time
        :param path: path to file or directory
        :type path: str
        :rtype: str
        """

        return self.get_description(path).update_date

    def __build_obj_description(self, path, list_status):
        """
        Builds description of file
        :rtype FileDescriptor
        """
        res = None
        mdtm_string = self.__ftp_driver.sendcmd('MDTM ' + path)
        datetime_str = mdtm_string[4:] #skip erorr code
        datetime_str = datetime_str[14:] #skipp all last characters like milliseconds. 14 digits - YYYYmmddHHmmss
        date = datetime.strptime(datetime_str,
                                 "%Y%m%d%H%M%S") if not self.is_directory(path) else None
        for obj_stat in list_status:
            obj_stat = re.split("[ ]+", obj_stat)
            if obj_stat[-1] == os.path.basename(path):
                res = FileDescriptor(name=path,
                                     update_date=date,
                                     size=self.size(path))
        return res

    def close(self):
        """
        Closes the FTP/FTPS session and its underlying channel
        """
        self.__ftp_driver.quit()


class FTPSConnector(FTPConnector):
    """
    Connector for FTP tls server
    """

    @staticmethod
    def get_session_ftps(host, login=None, password=None, port=21, auth=False, protocol=True):
        """
        Creates connection with FTPS server
        :param host: host of FTPS server
        :param login: user's name
        :param password: password for user
        :param port: port of FTPS server
        :param auth: if it is true sets up secure control
        connection by using TLS/SSL
        :param protocol: if it is true sets up secure data connection
        else sets up clear text data connection
        :return: FTPConnector
        :type host: str
        :type login: str
        :type password: str
        :type port: int
        :type auth: bool
        :type protocol: bool
        :rtype: FTPConnector
        """
        try:
            ftp = FTP_TLS()
            ftp.connect(host, port)
            ftp.login(login, password)
            if protocol:
                ftp.prot_p()
            else:
                ftp.prot_c()
            if auth:
                ftp.auth()
            return FTPConnector(ftp)
        except error_perm, exp:
            raise FTPConnectorError(
                exp.message
            )


class SFTPConnector(object):
    """
    Connector for SFTP server
    """

    @staticmethod
    def get_session_sftp(host, login=None, password=None, hkey_path=None):
        """
        Creates connection with SFTP server
        :param host: host of SFTP server
        :param login: user's name
        :param password: password for user
        :param hkey_path: path to ssh key
        :return: SFTPConnector
        :type host: str
        :type login: str
        :type password: str
        :type hkey_path: str
        :rtype: SFTPConnector
        """
        import paramiko
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys(filename=hkey_path)
        ssh_client.connect(hostname=host, username=login, password=password)
        sftp = paramiko.SFTPClient.from_transport(ssh_client.get_transport())

        return SFTPConnector(sftp, ssh_client)

    def __normalize_path(self, path):
        """
        Validates path: remove '/' if we have pass not to root.
        '/' means path to root directory
        :param path: path to file or directory
        :return: normalized path
        :type path: str
        :rtype: str
        """
        return path[:-1] if len(path) > 1 and path[-1] == "/" else path

    def __get_abs_path(self, path):
        """
        Returns absolutely path
        :param path: path to file or directory
        :return: absolutely path
        :type path: str
        :rtype: str
        """
        path = self.__normalize_path(path)
        return self.__ftp_instance.normalize(path)

    def __assert_is_not_dir(self, path):
        """
        Checks if file is not directory
        :param path: path to file or directory
        :type path: str
        :raise FTPFileError
        """
        if self.is_directory(path):
            raise FTPFileError("{0} is directory".format(path))

    def __assert_is_dir(self, path):
        """
        Checks if file is directory
        :param path: path to file or directory
        :type path: str
        :raise FTPFileError
        """
        if not self.is_directory(path):
            raise FTPFileError("{0} is file".format(path))

    def __assert_is_not_exists(self, path):
        """
        Checks if file is not exists
        :param path: path to file or directory
        :type path: str
        :raise FileNotFoundException:
        """
        if self.exists(path):
            raise FTPFileError(
                "'{path}' is exists".format(path)
            )

    def __assert_exists(self, path):
        """
        Checks if file is exists
        :param path: path to file or directory
        :type path: str
        :raise FileNotFoundException:
        """
        if not self.exists(path):
            raise FileNotFoundException(
                "'{path}' is not exists".format(path)
            )

    def exists(self, path):
        """
        Checks if file at the given path is exists
        :param path: path to file or directory
        :type path: str
        :rtype : bool
        """
        path = self.__normalize_path(path)
        try:
            self.__ftp_instance.stat(path)
        except IOError:
            return False
        else:
            return True

    def __get_name(self, path):
        """
        Gets name of file at the given path
        :param path: path to file or directory
        :type path: str
        :rtype: str
        """
        return os.path.basename(path)

    def __get_basename(self, path):
        """
        Gets path to basedir of file at the given path
        :param path: path to file or directory
        :type path: str
        :rtype: str
        """
        return os.path.dirname(path) if os.path.dirname(path) != "" else "/"

    def size(self, path):
        """
        Returns size from file at the given path.
        Returns 0 for folder
        :param path: path to file or directory
        :type path: str
        :rtype: long
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        return 0 if self.is_directory(path) else self.__ftp_instance.stat(path).st_size

    def list_files(self, path):
        """
        Returns a list containing the names of the entries
        in the given path
        :param path: path to file or directory
        :type path: str
        :rtype list, str
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        return [os.path.join(path, tmp) for tmp in self.__ftp_instance.listdir(path)] \
            if self.is_directory(path) else path

    def create(self, path, make_dir=False, create_parents=False):
        """
        Creates file at the given path.
        Creates directory if parameter 'make_dir' is True.
        Creates all parent`s directory if create_parents is True
        :param path: path to file or directory
        :param make_dir: creates directory if this param is True
        :param create_parents: creates all parents directory if is True
        :return: FTPConnector
        :type path: str
        :type make_dir: bool
        :type create_parents: bool
        :rtype SFTPConnector
        """
        path = self.__normalize_path(path)
        return self.create_dir(path) if make_dir else \
            self.create_file(path, create_parents)

    def create_dir(self, path):
        """
        Creates directory at the given path if it doesn't exist.
        Creates all parent`s directory
        :param path: path to directory
        :type path: str
        :rtype SFTPConnector
        """
        path = self.__normalize_path(path)
        if not self.exists(self.__get_basename(path)):
            self.create_dir(self.__get_basename(path))
        self.__assert_is_not_exists(path)
        self.__ftp_instance.mkdir(path)
        return self

    def create_file(self, path, create_parents=False):
        """
        Creates file at the given path if it doesn't exist.
        Creates all parent`s directory if create_parents is True
        :param path: path to file
        :param create_parents: creates all parents directory if is True
        :type path: str
        :type create_parents: bool
        :rtype SFTPConnector
        """
        path = self.__normalize_path(path)
        if create_parents and not self.exists(self.__get_basename(path)):
            self.create_dir(self.__get_basename(path))
        self.__assert_exists(self.__get_basename(path))
        self.__assert_is_not_exists(path)
        self.upload(path, os.path.join(os.path.dirname(__file__), 'resources/zero'))
        return self

    def download_file(self, path, local_path):
        """
        Copies a remote file (path) from the SFTP server
        to the local host as local_path
        :param path: path to file on ftp
        :param local_path: path to future file or existing directory
        :type path: str
        :type local_path: str
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        local_path = self.__normalize_path(local_path)
        self.__assert_is_not_dir(path)

        if LocalFS(local_path).exists():
            _local_dst = local_path if not LocalFS(local_path).is_directory() else \
                os.path.join(local_path, self.__get_name(path))
            self.__ftp_instance.get(remotepath=path, localpath=_local_dst)
        elif LocalFS(self.__get_basename(local_path)).exists():
            local = LocalFS(self.__get_basename(local_path))
            if local.is_directory():
                self.__ftp_instance.get(remotepath=path, localpath=local_path)
            else:
                raise FTPFileError("'{0}' is not directory".format(local_path))
        else:
            raise FTPFileError("'{0}' is not exists".format(local_path))

    def download_dir(self, path, local_path, predicate=lambda path, connector: True, recursive=True):
        """
        Copies a remote directory (path) with files from the SFTP server
        to the local host into a local_path. In addition, copies inner directories
        if parameter 'recursive' is True. Filters file if predicate was given
        :param path: path to directory on ftp
        :param local_path: path to directory on local file system
        :param predicate: predicate for filter file
        :param recursive: copies all inner directory at the given path if is True
        :type path: str
        :type local_path: str
        :type recursive: bool
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        self.__assert_is_dir(path)
        local_path = self.__normalize_path(local_path)
        LocalFS(local_path).assert_exists()
        LocalFS(local_path).assert_is_dir()
        local_path = os.path.join(local_path, self.__get_name(path))
        LocalFS(local_path).create_directory()
        list_ = self.list_files(path)
        if predicate:
            list_ = [path for path in list_ if predicate(path, self)]
        for tmp in list_:
            if recursive and self.is_directory(tmp):
                self.download_dir(tmp, local_path, predicate, recursive)
            elif not self.is_directory(tmp):
                self.download_file(tmp, local_path)

    def upload(self, path, local_path, update=False):
        """
        Copies a local file (local_path) to the SFTP server as path.
        Updates exists file if parameter 'update' is True
        :param path: path to file on ftp
        :param local_path: path to file on local file system
        :param update: updates file on ftp if is True
        :type path: str
        :type local_path: str
        :type update: bool
        """
        path = self.__normalize_path(path)
        local_path = self.__normalize_path(local_path)
        LocalFS(local_path).assert_exists()
        if self.exists(path):
            if not self.is_directory(path):
                if update:
                    self.__ftp_instance.put(localpath=local_path, remotepath=path)
                else:
                    raise FTPFileError("'{0}' is exists".format(path))
            else:
                path_name = self.__get_name(local_path)
                self.__ftp_instance.put(localpath=local_path,
                                        remotepath=os.path.join(path, path_name))
        else:
            path_name = self.__get_basename(path)
            if self.exists(path_name):
                self.__ftp_instance.put(localpath=local_path, remotepath=path)
            else:
                raise FileNotFoundException("'{path}' does not exists"
                .format(path=path_name))

    def base_dir(self, path):
        """
        Gets path to base directory of file at the given path
        :param path: path to file
        :return: path to base directory
        :type path: str
        :rtype: str
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        path = self.__get_abs_path(path)
        return self.__get_basename(path)

    def delete(self, path, recursive=False):
        """
        Removes the files or empty directory at the given path.
        Deletes directory with files if parameter 'recursive' is True
        :param path: path to file or directory
        :param recursive: deletes directory with all files if is True
        :type path: str
        :type recursive: bool
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        if self.is_directory(path):
            if recursive:
                for tmp in self.list_files(path):
                    if self.is_directory(tmp):
                        self.delete(tmp, recursive=True)
                    else:
                        self.__ftp_instance.remove(tmp)
            self.__ftp_instance.rmdir(path)
        else:
            self.__ftp_instance.remove(path)

    def is_directory(self, path):
        """
        Checks if file at the given path is directory
        :param path: path to file or directory
        :type path: str
        :rtype bool
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        return S_ISDIR(self.__ftp_instance.stat(path).st_mode)

    def get_description(self, path):
        """
        Gets metadata of file at the given path
        :param path: path to file or directory
        :return: FileDescriptor with metadata of file at the given path
        :type path: str
        :rtype: FileDescriptor
        """
        path = self.__normalize_path(path)
        self.__assert_exists(path)
        s = self.__ftp_instance.stat(path)
        return FileDescriptor(name=path, update_date=datetime.fromtimestamp(s.st_mtime),
                              size=self.size(path))

    def modification_time(self, path):
        """
        Returns last modification time
        :param path: path to file or directory
        :type path: str
        :rtype: str
        """

        return self.get_description(path).update_date

    def close(self):
        """
        Closes the SFTP session and its underlying channel
        """
        self.__ftp_instance.close()

    def __init__(self, sftp, ssh_client):
        self.__ftp_instance = sftp
        self.__ssh_client = ssh_client
