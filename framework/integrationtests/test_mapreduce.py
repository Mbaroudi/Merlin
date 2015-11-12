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
import tempfile

from unittest2 import TestCase, skipUnless


from merlin.common.test_utils import has_command
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS
from merlin.tools.mapreduce import MapReduce

HADOOP_STREAMING_JAR = os.path.join(os.path.dirname(__file__), 'resources', 'mapreduce',
                                    'hadoop-streaming.jar')


class TestMapReduceJob(TestCase):
    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_mr_job_command_generation_with_arguments(self):
        _job_name = "test_mr_job_%s" % uuid.uuid4()

        _base_dir = HDFS(os.path.join("/tmp", _job_name))
        _base_dir.create_directory()
        try:
            jar = os.path.join(os.path.dirname(__file__), 'resources', 'mapreduce', 'hadoop-mapreduce-examples.jar')
            # configure job inputs
            _job_input = HDFS(os.path.join(_base_dir.path, "input"))
            _job_input.create_directory()
            LocalFS(os.path.join(
                os.path.dirname(__file__),
                'resources',
                'mapreduce', 'raw-data.txt')
            ).copy_to_hdfs(
                _job_input.path
            )

            # configure job output
            _job_output = HDFS(os.path.join(_base_dir.path, "output"))
            if not os.path.exists(jar):
                self.skipTest("'%s' not found" % jar)

            job = MapReduce.prepare_mapreduce_job(jar=jar,
                                                  main_class="wordcount",
                                                  name=_job_name) \
                .with_config_option("split.by", "'\\t'") \
                .with_number_of_reducers(3) \
                .with_arguments(
                _job_input.path,
                _job_output.path
            )
            _command_submission_result = job.run()
            _command_submission_result.if_failed_raise(AssertionError("Cannot run MR job"))
            _job_status = job.status()
            self.assertTrue(_job_status is not None and _job_status.is_succeeded(), "MR job Failed")
            self.assertTrue(_job_output.exists(), "Error: empty job output")
            #     check counters
            self.assertEqual(6, _job_status.counter(group='File System Counters',
                                                    counter='HDFS: Number of write operations'))
            self.assertEqual(1, _job_status.counter(group='Job Counters', counter='Launched map tasks'))
            self.assertEqual(3, _job_status.counter(group='Job Counters', counter='Launched reduce tasks'))
            self.assertEqual(2168, _job_status.counter(group='File Input Format Counters', counter='Bytes Read'))
        finally:
            _base_dir.delete_directory()


class TestMapReduceStreamingJob(TestCase):
    @classmethod
    def setUpClass(cls):
        path_to_file = os.path.join(os.path.dirname(__file__), 'resources',
                                 'mapreduce', 'mapper.py')
        cls._delete_carriage_return(path_to_file)
        path_to_file = os.path.join(os.path.dirname(__file__), 'resources',
                                 'mapreduce', 'reducer.py')
        cls._delete_carriage_return(path_to_file)

    @staticmethod
    def _delete_carriage_return(path_to_file):
        new_file = open(path_to_file + ".new", 'w')
        for line in open(path_to_file):
            line = line.replace('\r', '')
            new_file.write(line)
        new_file.close()
        os.remove(path_to_file)
        os.rename(new_file.name, path_to_file)

    def _run_and_assert(self, job):
        command_result = job.run()
        command_result.if_failed_raise(AssertionError("test_streaming_job_generated test failed"))
        _job_status = job.status()
        self.assertTrue(_job_status is not None and _job_status.is_succeeded())
        return _job_status

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_streaming_job(self):
        _job_basedir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            job = self._template_streaming_job_(base_dir=_job_basedir.path)
            command_result = job.run()
            command_result.if_failed_raise(AssertionError("test_streaming_job_generated test failed"))
            _job_status = job.status()
            self.assertTrue(_job_status is not None and _job_status.is_succeeded())
            # counters
            self.assertEqual(740, _job_status.counter(group='Map-Reduce Framework', counter='Spilled Records'),
                             "counters['Map-Reduce Framework']['Spilled Records']")
            self.assertEqual(143, _job_status.counter(group='Map-Reduce Framework', counter='Reduce output records'),
                             "counters['Map-Reduce Framework']['Reduce output records']")
            self.assertEqual(370, _job_status.counter(group='Map-Reduce Framework', counter='Reduce input records'),
                             "counters['Map-Reduce Framework']['Reduce input records']")
        finally:
            _job_basedir.delete_directory()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_streaming_job_with_multiple_inputs(self):
        _job_basedir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:

            job = self._template_streaming_job_(base_dir=_job_basedir.path)

            _additional_datasource = HDFS(os.path.join(_job_basedir.path, "input2"))
            _additional_datasource.create_directory()
            LocalFS(os.path.join(os.path.dirname(__file__), 'resources',
                                 'mapreduce', 'raw-data.txt')
            ).copy_to_hdfs(
                _additional_datasource.path)
            job.take(_additional_datasource.path)
            command_result = job.run()
            command_result.if_failed_raise(AssertionError("test_streaming_job_with_multiple_inputs test failed"))
            _job_status = job.status()
            self.assertTrue(_job_status is not None and _job_status.is_succeeded())
            # check counters
            self.assertEqual(740, _job_status.counter(group='Map-Reduce Framework', counter='Reduce input records'),
                             "counters['Map-Reduce Framework']['Reduce input records']")
        finally:
            _job_basedir.delete_directory()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def test_streaming_job_without_reducer(self):
        _job_basedir = HDFS(os.path.join("/tmp", str(uuid.uuid4())))
        try:
            job = self._template_streaming_job_(base_dir=_job_basedir.path, map_only_job=True)
            command_result = job.run()
            command_result.if_failed_raise(AssertionError("Cannot run map-only job"))
            _job_status = job.status()
            self.assertTrue(_job_status is not None and _job_status.is_succeeded())

            #   check counters
            self.assertEqual(2, _job_status.counter(group='Job Counters', counter='Launched map tasks'))
            self.assertEqual(11, _job_status.counter(group='Map-Reduce Framework', counter='Map input records'))
            self.assertEqual(3252, _job_status.counter(group='File Input Format Counters', counter='Bytes Read'))
        finally:
            _job_basedir.delete_directory()

    @skipUnless(has_command('hadoop'), "Hadoop client should be installed")
    def _template_streaming_job_(self, base_dir="/tmp", map_only_job=False):
        if not os.path.exists(HADOOP_STREAMING_JAR):
            self.skip("Cannot allocate %s" % HADOOP_STREAMING_JAR)
        _hdfs_basdir = HDFS(base_dir)
        if not _hdfs_basdir.exists():
            _hdfs_basdir.create_directory()
        _job_input = HDFS(os.path.join(_hdfs_basdir.path, "input"))
        _job_input.create_directory()
        _job_output = HDFS(os.path.join(_hdfs_basdir.path, "output"))
        home = os.path.dirname(__file__)
        _mapper = os.path.join(home, 'resources', 'mapreduce', 'mapper.py')
        _reducer = os.path.join(home, 'resources', 'mapreduce', 'reducer.py')

        LocalFS(
            os.path.join(os.path.dirname(__file__), 'resources', 'mapreduce', 'raw-data.txt')
        ).copy_to_hdfs(
            _job_input.path
        )

        return MapReduce.prepare_streaming_job(name="test-mr-streaming-job{}".format(str(uuid.uuid4())), jar=HADOOP_STREAMING_JAR) \
            .take(_job_input.path) \
            .process_with(mapper=_mapper, reducer=None if map_only_job else _reducer) \
            .save(_job_output.path)

