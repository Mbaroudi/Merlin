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
Wrappers and mocks for testing

"""
from mock import Mock

from merlin.common.shell_command_executor import execute_shell_command, \
    build_command, Result


def has_command(command):
    """
    Wrapper for  Unix command used to identify the location of executables.
    Can be used to skip integration tests
    :param command: programname
    :return:
    """
    print "CONFIGURE TEST CASES.....", str(execute_shell_command("which", command).is_ok())
    return execute_shell_command("which", command).is_ok()


def mock_executor(expected_command, status=0, stdout=None, stderr=None):
    """
    Mock command executor: throttles command invocation
    Uses expected_command parameter to validate command generation logic
    :param expected_command:
    :param status: exitcode which should be return from executor
    :param stdout: stdout which should be return from executor
    :param stderr:  stderr which should be return from executor
    :return:
    """

    def executor(cmd, *args):
        _actual_cmd = build_command(cmd, *args)
        if expected_command != _actual_cmd:
            raise AssertionError(
                "ERROR: expected and actual command are different: \n\tEXPECTED: "
                "{0}\n\tACTUAL:   {1}".format(
                    expected_command, _actual_cmd))
        result = Mock(spec=Result, status=status, stdout=stdout, stderr=stderr)
        return result

    return executor
