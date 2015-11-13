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
import uuid

from unittest2 import TestCase
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore

from merlin.common.shell_command_executor import build_command
from merlin.tools.mapreduce import MapReduce


class TestMapReduceCommandGeneration(TestCase):
    def assert_generated_command(self, expected_cmd):
        def executor(cmd, *args):
            actual_command = build_command(cmd, *args)
            self.assertEqual(expected_cmd, actual_command,
                             "\n\nEXPECTED : %s \nACTUAL   : %s" % (expected_cmd, actual_command))

        return executor


class TestStreamingMapReduceCommandGenerationViaDsl(TestMapReduceCommandGeneration):
    def test_streaming_job(self):
        _job_name = "test_streaming_job_%s" % uuid.uuid4()
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-mapper mapper.py ' \
                            '-reducer reducer.py ' \
                            '-input data ' \
                            '-output output.txt'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).take(
            "data"
        ).process_with(
            mapper="mapper.py",
            reducer="reducer.py").save("output.txt").run()

    def test_streaming_job_without_reducers(self):
        _job_name = "test_streaming_job_%s" % uuid.uuid4()
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-mapper mapper.py ' \
                            '-reducer NONE ' \
                            '-numReduceTasks 0 ' \
                            '-input data ' \
                            '-output output.txt'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).take(
            "data"
        ).map_with(
            mapper="mapper.py"
        ).disable_reducers().save("output.txt").run()

    def test_streaming_map_only_job_generation(self):
        _config_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources',
            'mapreduce',
            'mapreduce_streaming_job.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore=metastore)
        _job_name = 'streaming_test_job_map_only'
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-D value.delimiter.char=, ' \
                            '-D partition.to.process=20142010 ' \
                            '-mapper smapper.py ' \
                            '-reducer NONE ' \
                            '-numReduceTasks 0 ' \
                            '-input /raw/20102014 ' \
                            '-output /core/20102014'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            config=_config,
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).run()

    def test_generate_job_cmd_with_config_injections(self):
        _job_name = "test_streaming_job_%s" % uuid.uuid4()
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-D value.delimiter.char=, ' \
                            '-D partition.to.process=20142010 ' \
                            '-mapper mapper.py ' \
                            '-reducer reducer.py ' \
                            '-input data ' \
                            '-output output.txt'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).take(
            'data'
        ).process_with(
            mapper='mapper.py',
            reducer='reducer.py'
        ).save(
            'output.txt'
        ).with_config_option(
            key='value.delimiter.char',
            value=','
        ).with_config_option(
            key='partition.to.process',
            value='20142010'
        ).run()

    def test_generate_cmd_with_input_output_format(self):
        _job_name = "test_streaming_job_%s" % uuid.uuid4()
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-mapper mapper.py ' \
                            '-reducer reducer.py ' \
                            '-numReduceTasks 10 ' \
                            '-input data ' \
                            '-output output.txt ' \
                            '-inputformat org.apache.hadoop.mapred.KeyValueTextInputFormat ' \
                            '-outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).process_with(mapper='mapper.py', reducer='reducer.py', reducer_num=10).use(
            inputformat='org.apache.hadoop.mapred.KeyValueTextInputFormat',
            outputformat='org.apache.hadoop.mapred.SequenceFileOutputFormat'
        ).take('data').save('output.txt').run()

    def test_generate_cmd_with_partitioner(self):
        _job_name = "test_streaming_job_%s" % uuid.uuid4()
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-D map.output.key.field.separator=| ' \
                            '-D mapreduce.partition.keypartitioner.options=-k1,2 ' \
                            '-mapper mapper.py ' \
                            '-reducer reducer.py ' \
                            '-numReduceTasks 0 ' \
                            '-input data ' \
                            '-output output.txt ' \
                            '-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)

        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).take('data').process_with(mapper='mapper.py', reducer='reducer.py', reducer_num=0).use(
            partitioner='org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'
        ).save('output.txt').with_config_option(
            key='map.output.key.field.separator',
            value='|').with_config_option(
            key='mapreduce.partition.keypartitioner.options',
            value='-k1,2').run()


class TestStreamingMapReduceCommandGenerationFromIni(TestMapReduceCommandGeneration):
    def __init__(self, methodName='runTest'):
        super(TestStreamingMapReduceCommandGenerationFromIni, self).__init__(methodName)
        _config_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources',
            'mapreduce',
            'mapreduce_streaming_job.ini')
        metastore = IniFileMetaStore(file=_config_file)
        self._config = Configuration.load(metastore=metastore)

    def test_streaming_job_generation(self):
        _job_name = 'streaming_test_job'
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-mapper smapper.py ' \
                            '-reducer sreducer.py ' \
                            '-input /raw/20102014 ' \
                            '-output /core/20102014'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            config=self._config,
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).run()

    def test_streaming_map_only_job_generation(self):
        _job_name = 'streaming_test_job_map_only'
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-D value.delimiter.char=, ' \
                            '-D partition.to.process=20142010 ' \
                            '-mapper smapper.py ' \
                            '-reducer NONE ' \
                            '-numReduceTasks 0 ' \
                            '-input /raw/20102014 ' \
                            '-output /core/20102014'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            config=self._config,
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).run()

    def test_streaming_job_with_multiple_inputs(self):
        _job_name = 'streaming_test_job_with_multiple_inputs'
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-files dim1.txt ' \
                            '-libjars mr_001.jar,mr_002.jar ' \
                            '-mapper smapper.py ' \
                            '-reducer sreducer.py ' \
                            '-numReduceTasks 100 ' \
                            '-input /raw/20102014 ' \
                            '-input /raw/21102014 ' \
                            '-input /raw/22102014 ' \
                            '-output /core/20102014 ' \
                            '-inputformat \'org.mr.CustomInputFormat\' ' \
                            '-outputformat \'org.mr.CustomOutputFormat\' ' \
                            '-cmdenv JAVA_HOME=/java ' \
                            '-cmdenv tmp.dir=/tmp/streaming_test_job_with_multiple_inputs'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            config=self._config,
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).run()

    def test_load_streaming_job_with_config_injections(self):
        _job_name = 'streaming_test_job_with_custom_configurations'
        _expected_command = 'hadoop jar ' \
                            '{0}/resources/mapreduce/hadoop-streaming.jar ' \
                            '-D mapreduce.job.name={1} ' \
                            '-D value.delimiter.char=, ' \
                            '-D partition.to.process=20142010 ' \
                            '-mapper smapper.py ' \
                            '-reducer sreducer.py ' \
                            '-input /raw/20102014 ' \
                            '-output /core/20102014'\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_streaming_job(
            jar='{0}/resources/mapreduce/hadoop-streaming.jar'
            .format(os.path.dirname(os.path.realpath(__file__))),
            config=self._config,
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)).run()


class TestMapReduceCommandGenerationViaDsl(TestMapReduceCommandGeneration):
    def test_mr_job_command_generation(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()
        _expected_command = "hadoop jar {0}/resources/mapreduce/hadoop-mapreduce-examples.jar demo.mr.Driver -D mapreduce.job.name={1}"\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="demo.mr.Driver",
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)).run()

    def test_mr_job_command_generation_with_one_reducer(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()
        _expected_command = "hadoop jar {0}/resources/mapreduce/hadoop-mapreduce-examples.jar demo.mr.Driver -D mapreduce.job.name={1} -D mapreduce.job.reduces=1"\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"\
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="demo.mr.Driver",
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).with_number_of_reducers(1).run()

    def test_mr_job_command_generation_without_reducer(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()
        _expected_command = "hadoop jar {0}/resources/mapreduce/hadoop-mapreduce-examples.jar demo.mr.Driver -D mapreduce.job.name={1} -D mapreduce.job.reduces=0"\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="demo.mr.Driver",
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).disable_reducers().run()

    def test_mr_job_command_generation_with_configurations(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()
        _expected_command = "hadoop jar " \
                            "{0}/resources/mapreduce/hadoop-mapreduce-examples.jar " \
                            "demo.mr.Driver " \
                            "-D mapreduce.job.name={1} " \
                            "-D job.input=/data/raw/24102014 " \
                            "-D mapreduce.job.reduces=10"\
            .format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="demo.mr.Driver",
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).with_config_option("job.input", "/data/raw/24102014").with_number_of_reducers(10).run()

    def test_mr_job_command_generation_with_arguments(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()
        _expected_command = "hadoop jar " \
                            "{0}/resources/mapreduce/hadoop-mapreduce-examples.jar " \
                            "wordcount " \
                            "-D mapreduce.job.name={1} " \
                            "-D split.by='\\t' " \
                            "-D mapreduce.job.reduces=3 " \
                            "/user/vagrant/dmode.txt " \
                            "/tmp/test".format(os.path.dirname(os.path.realpath(__file__)),
                            _job_name)
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="wordcount",
            name=_job_name,
            executor=self.assert_generated_command(_expected_command)
        ).with_config_option("split.by", "'\\t'") \
            .with_number_of_reducers(3) \
            .with_arguments() \
            .run("/user/vagrant/dmode.txt", "/tmp/test")


class TestMapReduceCommandGenerationFromIni(TestMapReduceCommandGeneration):
    def __init__(self, methodName='runTest'):
        super(TestMapReduceCommandGenerationFromIni, self).__init__(methodName)
        _config_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'resources',
            'mapreduce',
            'mapreduce_streaming_job.ini')
        metastore = IniFileMetaStore(file=_config_file)
        self._config = Configuration.load(metastore=metastore)

    def test_mr_job_command_generation(self):
        _expected_command = 'hadoop jar {0}/resources/mapreduce/hadoop-mapreduce-examples.jar ' \
                            'test.mr.Driver ' \
                            '-D mapreduce.job.name=simple_mr_job ' \
                            '-D value.delimiter.char=, ' \
                            '-D partition.to.process=20142010 ' \
                            '/input/dir ' \
                            '/output/dir'.format(os.path.dirname(os.path.realpath(__file__)))
        MapReduce.prepare_mapreduce_job(
            jar="{0}/resources/mapreduce/hadoop-mapreduce-examples.jar"
            .format(os.path.dirname(os.path.realpath(__file__))),
            main_class="test.mr.Driver",
            config=self._config,
            name='simple_mr_job',
            executor=self.assert_generated_command(_expected_command)
        ).run()
