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
DistCp client

DistCp (distributed copy) is a tool used for large inter/intra-cluster copying.
It uses MapReduce to effect its distribution, error handling and recovery, and reporting.
It expands a list of files and directories into input to map tasks,
each of which will copy a partition of the files specified in the source list.

This client provides Python wrapper for Hadoop's distcp command-line interface

DISTCP JOB EXAMPLES :

Runs simple DistCp Job :
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -p hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar


Controlling the copy parallelism (using 12 parallel tasks):
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").use(mappers=12).run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -p -m 12 hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar


Uses update strategy:
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").update_destination().run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -p -update hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar


Uses overwrite strategy:
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").overwrite_destination().run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -p -overwrite hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar


Synchronizes destination with source using overwrite (or using update) strategy :
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").
        overwrite_destination(synchronize=True).run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -p -overwrite -delete hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar


Sets preserve replication number in next format -pr :
        DistCp().take("hdfs://localhost:8020/tmp/foo").
        copy_to("hdfs://localhost:8020/tmp/bar").
        preserve_replication_number().run()

    Will be transformed to next DistCp CLI command :
    hadoop distcp -pr hdfs://localhost:8020/tmp/foo hdfs://localhost:8020/tmp/bar

    Also can sets other preserves in format -p'X', where 'X' means letter, using next method :
        .preserve_block_size() Block size - 'b'
        .preserve_user() User - 'u'
        .preserve_permission() Permission - 'p'
        .preserve_group() Group - 'g'
        .preserve_checksum_type() Checksum type - 'c'
        .preserve_acl() Acl - 'a'
        .preserve_xattr() XAttr - 'x'

"""

from merlin.common.exceptions import DistCpError
from merlin.common.shell_command_executor import execute_shell_command
from merlin.common.logger import get_logger


class DistCp(object):
    """Hadoop's command distcp utilities."""
    LOG = get_logger("DistCP")

    def __init__(self, executor=execute_shell_command):
        """
        Creates a new DistCp instance
        :param executor: command executor
        :type executor:
        :rtype : DistCp
        """
        self.preserve = "-p"
        self.strategy = None
        self.mappers = None
        self.synchronize = False
        self.path_src = None
        self.path_dest = None
        self.__executor = executor

    def run(self):
        """
        Runs DistCp Job
        :rtype: Result
        """
        DistCp.LOG.info("Running DistCp Job")
        _process = self.__executor('hadoop distcp', self.build())
        _process.if_failed_raise(DistCpError("DistCp Job failed"))
        return _process

    def build(self):
        """
        Builds DistCp command
        :rtype: str
        """

        list_attributes = [self.preserve]
        if self.mappers:
            list_attributes.append(self.mappers)
        if self.strategy:
            list_attributes.append(self.strategy)
        if self.synchronize:
            list_attributes.append("-delete")
        if self.path_src:
            list_attributes.append(self.path_src)
        else:
            raise DistCpError("You must specify source that will be copied")
        if self.path_dest:
            list_attributes.append(self.path_dest)
        else:
            raise DistCpError("You must specify destination where will saved file")

        return " ".join(list_attributes)

    def take(self, path):
        """
        Specifies the directory or file on file system which will be copied.
        Exception will be raised in case the directory or file isn't exists on file system
        :param path: path to source which should be copied
        :type path: str
        :rtype: DistCp
        """
        self.path_src = path

        return self

    def copy_to(self, path):
        """
        Specifies the directory or file on file system into which the data should be copied.
        :param path: path to destination into which the data should be copied
        :type path: str
        :rtype: DistCp
        """
        self.path_dest = path

        return self

    def use(self, mappers=None):
        """
        Specifies number of mappers
        :param mappers: number of map tasks
        :type mappers: str, int
        :rtype: DistCp
        """
        self.mappers = "-m {0}".format(str(mappers))

        return self

    def update_destination(self, synchronize=False):
        """
        Changes command strategy to update
        :param synchronize: synchronizes source with destination if param is True
        :type synchronize: bool
        :rtype: DistCp
        """
        self.strategy = "-update"
        if synchronize:
            self.synchronize = True

        return self

    def overwrite_destination(self, synchronize=False):
        """
        Changes command strategy to overwrite
        :param synchronize: synchronizes source with destination if param is True
        :type synchronize: bool
        :rtype: DistCp
        """
        self.strategy = "-overwrite"
        if synchronize:
            self.synchronize = True

        return self

    def preserve_replication_number(self):
        """
        Sets replication number of file in destination equals
        to replication number of file in source
        :rtype: DistCp
        """

        self.__set_preserve('r')

        return self

    def preserve_block_size(self):
        """
        Sets block size of file in destination equals
        to block size of file in source
        :rtype: DistCp
        """
        self.__set_preserve('b')

        return self

    def preserve_user(self):
        """
        Sets user of file in destination equals
        to user of file in source
        :rtype: DistCp
        """
        self.__set_preserve('u')

        return self

    def preserve_permission(self):
        """
        Sets permission of file in destination equals
        to permission of file in source
        :rtype: DistCp
        """
        self.__set_preserve('p')

        return self

    def preserve_group(self):
        """
        Sets group of file in destination equals
        to group of file in source
        :rtype: DistCp
        """
        self.__set_preserve('g')

        return self

    def preserve_checksum_type(self):
        """
        Sets checksum type of file in destination equals
        to checksum type of file in source
        :rtype: DistCp
        """
        self.__set_preserve('c')

        return self

    def __set_preserve(self, value):
        """
        :return:
        """

        if value not in self.preserve:
            self.preserve = "{0}{1}".format(self.preserve, value)





