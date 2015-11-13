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
Wrapper for hadoop command line interface.

"""
import os
import re

from merlin.common.exceptions import CommandFailedError
import merlin.common.shell_command_executor as shell
from merlin.tools.distcp import DistCp


ROOT_DIR = '/'

PROTECTED_FOLDERS = [ROOT_DIR]


def mkdir(path, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -mkdir <paths> command.
    Create directory in the given path
    :param path: The directory path.
    :return:
    """
    return executor("hadoop", "fs", "-mkdir", path)


def copy_to_local(path, localdst, executor=shell.execute_shell_command):
    """
    Wrapper for
    hadoop fs -copyFromLocal <src:localFileSystem> <dest:Hdfs>
    command
    Copies file from HDFS to the local file system.
    :param path: hdfs
    :param localdst:
    :return:
    """
    return executor("hadoop", "fs", "-copyToLocal", path, localdst)


def copy_from_local(localsrc, hdfsdst, executor=shell.execute_shell_command):
    """
    Wrapper for
    hadoop fs -copyFromLocal <src:localFileSystem> <dest:Hdfs>
    command.
    Copies file from local file system to HDFS.
    :param localsrc:
    :param hdfsdst:
    :return:
    """
    return executor("hadoop", "fs", "-copyFromLocal", localsrc, hdfsdst)


def copy(files, dest, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -cp <source> <dest> command.
    Copies files from source to destination.
    :param files:
    :param dest:
    :return:
    """
    sources = " ".join(files) if isinstance(files, list) else files
    return executor("hadoop", "fs", "-cp", sources, dest)


def move(files, dest, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -mv <src> <dest> command.
    Move file from source to destination.
    This command allows multiple sources as well
    in which case the destination needs to be a directory.
    Moving files across filesystems is not permitted.
    :param files:
    :param dest:
    :return:
    """
    sources = " ".join(files) if isinstance(files, list) else files
    return executor("hadoop", "fs", "-mv", sources, dest)


def is_file_exists(path, executor=shell.execute_shell_command):
    """
    Checks if the file exists on HDFS.
    :param path:
    :return: boolean True, if the file denoted by this path exists
    else the method returns False
    """
    result = executor("hadoop", "fs", "-test", "-e", path)
    return hasattr(result, "status") and result.status == 0


def is_dir(path, executor=shell.execute_shell_command):
    """
    Checks whether the file denoted by this abstract path is a directory.
    :param path:
    :return: True if the file denoted by this path is a directory
    else the method returns False.
    """
    result = executor("hadoop", "fs", "-test", "-d", path)
    return hasattr(result, "status") and result.status == 0


def list_files(path, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -ls <path> command.
    List the file for the specified path
    :param path:
    :return:
    """
    result = executor('hadoop', 'fs', '-ls', path)
    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    files = [x.split(' ')[-1] for x in str(result.stdout).split('\n')
             if x and not found_line(x)]
    return files


def recursive_list_files(path, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -ls <path> command.
    List the file for the specified path
    :param path:
    :return:
    """
    result = executor('hadoop', 'fs', '-ls', '-R', path)
    found_line = lambda x: re.search('Found [0-9]+ items$', x)
    files = [x.split(' ')[-1] for x in str(result.stdout).split('\n')
             if x and not found_line(x)]
    return files


def file_size(path, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -du <path> command.
    Displays aggregate length of files contained in the directory
    or the length of a file in case its just a file.
    :param path:
    :return: the length of a file in bytes.
    """
    attributes = ['hadoop', 'fs', '-du']
    if is_dir(path):
        attributes.append("-s")
    attributes.append(path)
    result = executor(*attributes)
    result.if_failed_raise(CommandFailedError("Cannot get file size"))
    return int(str(result.stdout).split(" ")[0])


def get_merge(src, local_dst, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -getmerge <src> <localdst> command.
    Takes a source directory and a destination file as input
    and concatenates files in src into the destination local file.
    :param src: source directory
    :param local_dst: destination file
    """
    return executor('hadoop', 'fs', '-getmerge', src, local_dst)


def touchz(path, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -touchz <path> command
    Create a file of zero length.
    :param path: filename
    :return:
    """
    return executor('hadoop', 'fs', '-touchz', path)


def rm(path, recursive=False, executor=shell.execute_shell_command):
    """
    Wrapper for hadoop fs -rm -R <path> command
    Deletes a file.
    Non-empty directory will be removed only in case when recursive flag is True
    :param path: the path to the file to delete
    :param recursive: use recursive delete for non-empty directory
    :return:
    """
    if path in PROTECTED_FOLDERS:
        raise CommandFailedError("Cannot remove protected folder {0}".format(path))
    attributes = ['hadoop', 'fs', '-rm']
    if recursive:
        attributes.append('-R')
    attributes.append(path)
    return executor(*attributes)


def __stat_root_dir__(executor=shell.execute_shell_command):
    return executor(
        "hadoop",
        "fs",
        "-stat",
        "'drwxr-xr-x 0 %u %g %r %y  /'",
        ROOT_DIR
    )


def __stat_file__(path, executor=shell.execute_shell_command):
    return executor("hadoop", "fs", "-ls",
                    path if not is_dir(path)
                    else os.path.join(path, ".."))


def stat(path, executor=shell.execute_shell_command):
    """
    Returns information about the specified file with the following format:
        [permissions, number_of_replicas, userid,
        groupid, filesize, modification_date, modification_time, filename]

    :param path: file path
    :raise: CommandFailedError in case command failed
    """
    _result = __stat_root_dir__(executor) if path == ROOT_DIR else __stat_file__(path, executor)
    if _result is not None and _result.is_ok():
        r_stdout = [line
                    for line in str(_result.stdout).splitlines()
                    if line.strip().endswith(path)]
        if not r_stdout:
            raise CommandFailedError(
                "Cannot find file {0}. Command result: {1}".format(path, _result.stdout))
        return r_stdout[0].split() if r_stdout else None
    else:
        raise CommandFailedError(
            "Cannot get stat on a path : {0}".format(_result.stderr)
        )


def distcp(src, dest, strategy=None, num_mappers=None, executor=shell.execute_shell_command):
    """
    Copies files between clusters
    :param src: path to source
    :param dest: path to destination directory
    :param strategy: 'update' or 'overwrite'
    :param num_mappers: number of mappers
    :type src: str
    :type dest: str
    :type strategy: str
    :param num_mappers: str, int

    """
    dist = DistCp(executor=executor).take(src).copy_to(dest)
    if num_mappers:
        dist.use(mappers=num_mappers)
    if strategy == 'update':
        dist.update_destination()
    elif strategy == 'overwrite':
        dist.overwrite_destination()
    return dist.run()


def setfacl(path, acl_spec, executor=shell.execute_shell_command):
    """
    Sets ACLs for files and directories.
    :param path:  The path to the file or directory to modify.
    :param acl_spec: A comma-separated list of ACL entries.
    :param executor:
    :return:
    """
    return executor('hadoop', 'fs', '-setfacl', '-m', acl_spec, path)


def getfacl(path, executor=shell.execute_shell_command):
    """
    Returns the ACLs of files and directories.
    :param path: The path to the file or directory to list.
    :return: list of acls
    """
    return executor('hadoop', 'fs', '-getfacl', path)