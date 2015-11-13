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

from unittest2.case import TestCase
from merlin.common.configurations import Configuration
from merlin.common.exceptions import PigCommandError
from merlin.common.metastores import IniFileMetaStore
from merlin.common.test_utils import mock_executor
from merlin.tools.pig import Pig


class TestPig(TestCase):
    def test_run_script_from_file(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -f "wordcount.pig"')) \
            .run()

    def test_run_script_from_file_verbose(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -verbose -f "wordcount.pig"')) \
            .debug()

    def test_load_preconfigured_job(self):
        _command = 'pig -brief -optimizer_off SplitFilter -optimizer_off ColumnMapKeyPrune -e "ls /"'
        metastore = IniFileMetaStore(file=os.path.join(os.path.dirname(__file__), 'resources/pig/pig.ini'))
        pig = Pig.load_preconfigured_job(job_name='pig test',
                                         config=Configuration.load(
                                             metastore=metastore,
                                             readonly=False, accepts_nulls=True),
                                         command_executor=mock_executor(expected_command=_command))
        pig.without_split_filter().run()

    def test_wrap_with_quotes(self):
        _pc = Pig(config=Configuration.create(), job_name=None, command_executor=None)
        self.assertEqual("", _pc._wrap_with_quotes_(""))
        self.assertEqual(None, _pc._wrap_with_quotes_(None))
        self.assertEqual('"test"', _pc._wrap_with_quotes_("test"))
        self.assertEqual("'test'", _pc._wrap_with_quotes_("'test'"))
        self.assertEqual("'te\"st'", _pc._wrap_with_quotes_('te"st'))
        self.assertEqual('"te\'st"', _pc._wrap_with_quotes_("te'st"))

    def test_try_execute_empty_command(self):
        self.assertRaises(PigCommandError, Pig(
            config=Configuration.create(),
            job_name=None,
            command_executor=None).run)

    def test_run_script_from_string(self):
        Pig.load_commands_from_string(
            commands="ls /",
            command_executor=mock_executor('pig -e "ls /"')).run()

    def test_log4j_configs_injections(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-log4jconf ~/log4j.properties '
                                           '-f "wordcount.pig"')) \
            .log4j_config("~/log4j.properties") \
            .run()

    def test_configure_logging(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-logfile pig.log -brief -debug '
                                           '-f "wordcount.pig"')) \
            .log_config(logfile="pig.log", debug=True, brief=True) \
            .run()

    def test_with_param_query(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-param_file params.properties '
                                           '-f "wordcount.pig"')) \
            .load_parameters_from_file("params.properties") \
            .run()

    def test_with_param_file(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-param param001=value001 '
                                           '-param param002=value002 '
                                           '-x mapreduce '
                                           '-f "wordcount.pig"')) \
            .with_parameter("param001", "value001").using_mode() \
            .with_parameter("param002", "value002").run()

    def test_with_property_file(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-propertyFile pig.properties '
                                           '-x mapreduce '
                                           '-f "wordcount.pig"')) \
            .with_property_file("pig.properties").using_mode().run()

    def test_optimization_disabling(self):
        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off SplitFilter -f "wordcount.pig"')) \
            .without_split_filter().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off PushUpFilter -f "wordcount.pig"')) \
            .without_pushup_filter().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off MergeFilter -f "wordcount.pig"')) \
            .without_merge_filter().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off PushDownForeachFlatten -f "wordcount.pig"')) \
            .without_push_down_foreach_flatten().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off LimitOptimizer -f "wordcount.pig"')) \
            .without_limit_optimizer().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off ColumnMapKeyPrune -f "wordcount.pig"')) \
            .without_column_map_key_prune().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off AddForEach -f "wordcount.pig"')) \
            .without_add_foreach().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off MergeForEach -f "wordcount.pig"')) \
            .without_merge_foreach().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off GroupByConstParallelSetter -f "wordcount.pig"')) \
            .without_groupby_const_parallel_setter().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig -optimizer_off All -f "wordcount.pig"')) \
            .disable_all_optimizations().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-optimizer_off LimitOptimizer '
                                           '-optimizer_off AddForEach '
                                           '-f "wordcount.pig"')) \
            .without_add_foreach().without_limit_optimizer().run()

        Pig.load_commands_from_file(
            path='wordcount.pig',
            command_executor=mock_executor('pig '
                                           '-x tez '
                                           '-optimizer_off LimitOptimizer '
                                           '-optimizer_off AddForEach '
                                           '-no_multiquery '
                                           '-f "wordcount.pig"')) \
            .without_add_foreach().using_mode(type="tez")\
            .without_limit_optimizer() \
            .without_multiquery().run()


