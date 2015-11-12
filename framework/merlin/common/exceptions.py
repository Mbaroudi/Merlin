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
Specific exceptions

"""


class CommandException(Exception):
    """
    Thrown when command has failed.
    This class is the general class for command exceptions.
    """
    pass


class FileSystemException(Exception):
    """
    Thrown when a file system operation fails.
    This class is the general class for file system exceptions.
    """
    pass


class FileNotFoundException(FileSystemException):
    """
    Thrown when an attempt is made to access a file that does not exist.
    """
    pass


class AccessDeniedException(FileSystemException):
    """
    Thrown when access to a file is denied due to a file permission.
    """
    pass


class MapReduceConfigurationError(Exception):
    """Error which indicates some inconsistency in the configuration of the MapReduce job."""
    pass


class MapReduceJobException(Exception):
    """The base class for MapReduce Job Exceptions."""
    pass


class CommandFailedError(Exception):
    """
    Exception thrown when attempting to retrieve the result of the failed task
    """
    pass


class HiveCommandError(Exception):
    """
    Exception thrown when hive task failed
    """
    pass


class PigCommandError(Exception):
    """
    Exception thrown when pig task failed
    """
    pass


class SqoopCommandError(Exception):
    """
    Exception thrown when sqoop task failed
    """
    pass


class DistCpError(Exception):
    """
    Exception thrown when DistCP task failed
    """
    pass


class FTPConnectorError(Exception):
    """
    Exception thrown when client will has problem with connection to ftp server
    """
    pass


class FTPPredicateError(Exception):
    """
    Exception thrown when it has problem with predicate
    """
    pass


class FTPFileError(Exception):
    """
    Exception thrown when has problem with file on
    """
    pass

class ConfigurationError(Exception):
    """Error which indicates some inconsistency in the configuration of the service."""
    pass
