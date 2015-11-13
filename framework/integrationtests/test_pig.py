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

from tempfile import NamedTemporaryFile
import uuid

from unittest2.case import TestCase, skipUnless, skip
from merlin.common.configurations import Configuration
from merlin.common.shell_command_executor import execute_shell_command
from merlin.fs.hdfs import HDFS
from merlin.tools.pig import Pig, TaskOptions
from merlin.common.test_utils import has_command
TEZ_MODE_ENABLE = True


@skipUnless(has_command('pig'), "pig client should be installed")
class TestPigClient(TestCase):
    @skip("Parameter substitution does not supported while trying to run commands from string")
    def test_run_commands_from_string(self):
        _test_id = str(uuid.uuid4())
        _output_dir = "/tmp/data_{}".format(_test_id)
        _input_dir = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))

        commands = "A = load '$input_dir' using PigStorage(',');"
        commands += "B = foreach A generate \$0 as id;"
        commands += "STORE B into '$output_dir';"
        try:
            _pig = Pig.load_commands_from_string(commands) \
                .with_parameter("input_dir", _input_dir) \
                .with_parameter("output_dir", _output_dir)
            _result = _pig.run()
            _result.if_failed_raise(AssertionError("test_run_commands_from_string failed"))
            self.assertTrue(HDFS(_output_dir).exists(), "Cannot find job output")
        finally:
            self.delete_file_in_hdfs(_input_dir)
            self.delete_file_in_hdfs(_output_dir)

    def test_run_commands_from_string_without_param_substitution(self):
        _test_id = str(uuid.uuid4())
        _output_dir = "/tmp/data_{}".format(_test_id)
        _input_dir = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))

        commands = "A = load '{}' using PigStorage(',');".format(_input_dir)
        commands += "B = foreach A generate \$0 as id;"
        commands += "STORE B into '{}';".format(_output_dir)
        try:
            _pig = Pig.load_commands_from_string(commands)
            _result = _pig.run()
            _result.if_failed_raise(AssertionError("test_run_commands_from_string failed"))
            self.assertTrue(HDFS(_output_dir).exists(), "Cannot find job output")
        finally:
            self.delete_file_in_hdfs(_input_dir)
            self.delete_file_in_hdfs(_output_dir)

    def test_run_preconfigured_job_without_parameters_substitution(self):
        _test_id = str(uuid.uuid4())
        _job_name = "TEST_PIG_{}".format(_test_id)
        _input_dir = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))
        _output_dir = "/tmp/data_{}".format(_test_id)

        _commands = "A = load '{}' using PigStorage(',');".format(_input_dir)
        _commands += "B = foreach A generate \$0 as id;"
        _commands += "STORE B into '{}';".format(_output_dir)
        # create job configuration. can also be loaded from .ini file
        _config = Configuration.create()
        _config.set(_job_name, TaskOptions.CONFIG_KEY_COMMANDS_STRING, _commands)
        _config.set(_job_name, TaskOptions.CONFIG_KEY_LOG_BRIEF, 'enabled')
        _config.set(_job_name, TaskOptions.CONFIG_KEY_PARAMETER_VALUE,
                    'input_dir={}\noutput_dir={}'.format(_input_dir, _output_dir))
        try:
            _pig = Pig.load_preconfigured_job(config=_config, job_name=_job_name)
            _result = _pig.run()
            _result.if_failed_raise(AssertionError("test_run_preconfigured_job failed"))
            self.assertTrue(HDFS(_output_dir).exists(), "Cannot find job output")
        finally:
            self.delete_file_in_hdfs(_input_dir)
            self.delete_file_in_hdfs(_output_dir)

    @skip("Parameter substitution does not supported while trying to run commands from string")
    def test_run_preconfigured_job_with_parameters_substitution(self):
        _test_id = str(uuid.uuid4())
        _job_name = "TEST_PIG_{}".format(_test_id)
        _input_dir = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))
        _output_dir = "/tmp/data_{}".format(_test_id)

        _commands = "A = load '$input_dir' using PigStorage(',');"
        _commands += "B = foreach A generate \$0 as id;"
        _commands += "STORE B into '$output_dir';"
        # create job configuration. can also be loaded from .ini file
        _config = Configuration.create()
        _config.set(_job_name, TaskOptions.CONFIG_KEY_COMMANDS_STRING, _commands)
        _config.set(_job_name, TaskOptions.CONFIG_KEY_LOG_BRIEF, 'enabled')
        _config.set(_job_name, TaskOptions.CONFIG_KEY_PARAMETER_VALUE,
                    'input_dir={}\noutput_dir={}'.format(_input_dir, _output_dir))
        try:
            _pig = Pig.load_preconfigured_job(config=_config, job_name=_job_name)
            _result = _pig.run()
            _result.if_failed_raise(AssertionError("test_run_preconfigured_job failed"))
            self.assertTrue(HDFS(_output_dir).exists(), "Cannot find job output")
        finally:
            self.delete_file_in_hdfs(_input_dir)
            self.delete_file_in_hdfs(_output_dir)

    def test_run_commands_from_file(self):
        _test_id = str(uuid.uuid4())
        _inputs = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))
        commands = "A = load '$input_dir' using PigStorage(',');"
        commands += "B = foreach A generate \$0 as id;"
        commands += "STORE B into '$output_dir';"
        files_s = self.temp_file(commands)
        try:
            _output_dir = "/tmp/data_{}".format(_test_id)
            pig = Pig.load_commands_from_file(files_s) \
                .with_parameter("input_dir", _inputs) \
                .with_parameter("output_dir", _output_dir)
            self.assertTrue(pig.run().is_ok())
            self.assertTrue(HDFS(_output_dir).exists())
        finally:
            self.delete_local(files_s)
            self.delete_file_in_hdfs()
            self.delete_file_in_hdfs(_inputs)

    @skipUnless(TEZ_MODE_ENABLE, "pig client should have tez mode")
    def test_run_commands_from_file_on_tez(self):
        _test_id = str(uuid.uuid4())
        _inputs = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))
        commands = "A = load '$input_dir' using PigStorage(',');"
        commands += "B = foreach A generate \$0 as id;"
        commands += "STORE B into '$output_dir';"
        files_s = self.temp_file(commands)
        try:
            _output_dir = "/tmp/data_{}".format(_test_id)
            pig = Pig.load_commands_from_file(files_s) \
                .with_parameter("input_dir", _inputs) \
                .with_parameter("output_dir", _output_dir) \
                .using_mode(type="tez")
            self.assertTrue(pig.run().is_ok())
            self.assertTrue(HDFS(_output_dir).exists())
        finally:
            self.delete_local(files_s)
            self.delete_file_in_hdfs()
            self.delete_file_in_hdfs(_inputs)

    def test_logging_configuration(self):
        files = self.copy_file_from_local(self.temp_file("hello,world,world", ".txt"))
        path = "/tmp/pig_log"
        commands = "A = load '$input_dir' using PigStorage(',');"
        commands += "B = foreach A generate \$0 as id;"
        commands += "STORE B into '$output_dir';"
        files_s = self.temp_file(commands)
        try:
            import os

            os.makedirs(path)
            pig = Pig.load_commands_from_file(files_s).with_parameter("input_dir", files) \
                .with_parameter("output_dir", "/tmp/data")
            pig.log_config(logfile=path + "pig")
            self.assertEqual(os.path.exists(path), pig.run().is_ok())
        finally:
            import shutil

            shutil.rmtree(path)
            self.delete_file_in_hdfs()
            self.delete_file_in_hdfs(files)
            self.delete_local(files_s)

    def temp_file(self, msg=None, suffix=".pig"):
        f = NamedTemporaryFile(mode="w+b", suffix=suffix, delete=False, prefix="pig")
        f.write(msg if msg else "show tables")
        f.close()
        return f.name

    def delete_file_in_hdfs(self, path="/tmp/data"):
        execute_shell_command("hadoop", "fs", "-rm -R" if path == "/tmp/data" else "-rm", path)

    def delete_local(self, path):
        import os

        os.remove(path)

    def copy_file_from_local(self, path):
        execute_shell_command("hadoop", "fs", "-copyFromLocal", path, "/tmp/")
        import os

        os.remove(path)
        return "/tmp/" + os.path.split(path)[1]


