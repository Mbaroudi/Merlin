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
This module handles the execution of external processes.

"""
import subprocess

from merlin.common.logger import get_logger, logging


__log__ = get_logger('ShellCommandExecutor')


def build_command(command, *args):
    """ Creates command string"""
    cmd = [command] + list(args)
    cmd_line = " ".join(cmd)
    return cmd_line

def _process_(async):
    """wrapper for command execution function"""

    def wrapper(_function):
        """
        Executor for commands
        """
        def executor(command, *args):
            """
            Builds and executes commands
            """
            cmd_line = build_command(command, *args)
            __log__.info("Executing {0}".format(cmd_line))
            _process = subprocess.Popen(cmd_line,
                                        shell=True,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
            __result = Result(process=_process, async=async)
            if not async:
                __result.log(__log__)
            return __result

        return executor

    return wrapper


@_process_(async=False)
def execute_shell_command(command, *args):
    """
    Run shell command with arguments. Waits for command to complete, then
    return the command execution result
    :param command: command to call
    :type cmd: str
    :param args: command arguments
    :type args: list
    :return: result of the command execution
    """
    pass


@_process_(async=True)
def execute_shell_command_async(command, *args):
    """
    Run shell command.
    :param command: command to call
    :type cmd: str
    :param args: command arguments
    :type args: list
    :return: result of the command execution
    """
    pass


class Result(object):
    """ The result of the command submission."""

    def __init__(self, process, async):
        self._process = process
        self._async = async
        self._stdout, self._stderr = (None, None) if async else self._process.communicate()
        self._status = self._process.poll() if async else self._process.returncode

    def is_running(self):
        """
        Determine whether command is executing
        :return: A boolean indication of state : true if the command is running, otherwise false.
        """
        return self._process.poll() is None

    @property
    def stdout(self):
        """ Command standard output """
        return self._stdout

    @property
    def stderr(self):
        """Command standard error output"""
        return self._stderr

    @property
    def status(self):
        """Command Exit status."""
        return self._status

    @stdout.getter
    def stdout(self):
        """Returns command standard output"""
        self._update_state_()
        return self._stdout

    @stdout.getter
    def stderr(self):
        """Returns command standard error output"""
        self._update_state_()
        return self._stderr

    @status.getter
    def status(self):
        """Returns command exit status."""
        self._update_state_()
        return self._status

    def _update_state_(self):
        """Update status for command which is executed asynchronously"""
        if self._async and self._status is None and not self.is_running():
            self._stdout, self._stderr = self._process.communicate()
            self._status = self._process.returncode
            self.log(logger=__log__)

    def __getattr__(self, name):
        return self.__dict__[name] if name in self.__dict__ else None

    def log(self, logger, level=logging.DEBUG):
        """writes exit status, stdout and stderr to kog"""
        if logger and not self.is_running() and logger.isEnabledFor(level):
            logger.log(level, "STATUS : {0}".format(self.status))
            logger.log(level, "STDOUT : {0}".format(self.stdout))
            logger.log(level, "STDERR : {0}".format(self.stderr))

    def is_ok(self, success_status=0):
        """
        checks if command was executed successfully
        :param success_status: By convention, the value 0 indicates normal termination.
        :return: if the actual exit status is equal to expected status
        """
        return self.status == success_status

    def if_failed_raise(self, exception, success_status=0):
        """
        Checks if command was executed successfully otherwise exception will be thrown
        :param exception: exception to throw in case command status is not equal
                to expected exit status
        :param success_status: By convention, the value 0 indicates normal termination.
        :raise:
        """
        if not self.is_ok(success_status):
            if hasattr(self, "stderr"):
                raise type(exception)(
                    "{message} : {details}"
                    .format(message=exception.message, details=str(self.stderr))
                )
            else:
                raise exception

