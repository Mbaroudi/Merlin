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

import mock
from unittest2 import TestCase

from merlin.common.shell_command_executor import build_command, Result
from merlin.tools.mapreduce import JobStatus


class TestJobStatus(TestCase):
    def __init__(self, methodName='runTest'):
        super(TestJobStatus, self).__init__(methodName)

    def test_get_job_id(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'stderr')
        with open(_stderr) as _file:
            self.assertEqual('job_1412153770896_0078', JobStatus.job_id("\n".join(_file.readlines())),
                             "Cannot get job id from provided stderr")

    def test_get_job_status_processing(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'succeeded_job_status')
        with open(_stderr) as _file:
            job = JobStatus("test_job")
            job._parse_stdout_(stream="\n".join(_file.readlines()))
            status = job.job_stats
            self.assertTrue('Job' in status)
            self.assertTrue('Job File' in status)
            self.assertTrue('Job Tracking URL' in status)
            self.assertTrue('Uber job' in status)
            self.assertTrue('Number of maps' in status)
            self.assertTrue('Number of reduces' in status)
            self.assertTrue('map() completion' in status)
            self.assertTrue('reduce() completion' in status)
            self.assertTrue('Job state' in status)
            self.assertTrue('retired' in status)
            self.assertTrue('Counters' in status)
            self.assertTrue('File System Counters' in status[JobStatus.COUNTER_SECTION])
            self.assertTrue('FILE: Number of bytes read' in status[JobStatus.COUNTER_SECTION]['File System Counters'])
            self.assertEqual('1529',
                             status[JobStatus.COUNTER_SECTION]['File System Counters']['FILE: Number of bytes read'])

    def test_job_counters_processor(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'succeeded_job_status')
        with open(_stderr) as _file:
            job = JobStatus("test_job")
            job._parse_stdout_(stream="\n".join(_file.readlines()))
            self.assertTrue(job.is_succeeded())
            self.assertEqual(6, len(job.counters()), "Error : should contain 6 counter groups")
            self.assertEqual(49, sum([len(items) for items in job.counters().itervalues()]),
                             "Error : should contain 49 counters")
            # File System Counters
            self.assertEqual(1529, job.counter(group='File System Counters', counter='FILE: Number of bytes read'))
            self.assertEqual(289479, job.counter(group='File System Counters', counter='FILE: Number of bytes written'))
            self.assertEqual(0, job.counter(group='File System Counters', counter='FILE: Number of read operations'))
            self.assertEqual(1307, job.counter(group='File System Counters', counter='HDFS: Number of bytes written'))
            self.assertEqual(2, job.counter(group='File System Counters', counter='HDFS: Number of write operations'))
            # Job Counters
            self.assertEqual(2, job.counter(group='Job Counters', counter='Launched map tasks'))
            self.assertEqual(7619,
                             job.counter(group='Job Counters', counter='Total time spent by all reduce tasks (ms)'))
            self.assertEqual(7801856, job.counter(group='Job Counters',
                                                  counter='Total megabyte-seconds taken by all reduce tasks'))
            # Map-Reduce Framework
            self.assertEqual(11, job.counter(group='Map-Reduce Framework', counter='Map input records'))
            self.assertEqual(370, job.counter(group='Map-Reduce Framework', counter='Reduce input records'))
            self.assertEqual(827850752,
                             job.counter(group='Map-Reduce Framework', counter='Total committed heap usage (bytes)'))

            # Shuffle Errors
            self.assertEqual(0, job.counter(group='Shuffle Errors', counter='BAD_ID'))
            self.assertEqual(0, job.counter(group='Shuffle Errors', counter='WRONG_LENGTH'))
            self.assertEqual(0, job.counter(group='Shuffle Errors', counter='WRONG_REDUCE'))

            # File Input Format Counters
            self.assertEqual(3252, job.counter(group='File Input Format Counters', counter='Bytes Read'))
            # File Output Format Counters
            self.assertEqual(1307, job.counter(group='File Output Format Counters', counter='Bytes Written'))

            # Nonexisting counters
            self.assertEqual(None, job.counter(group='Dummy Group', counter='Dummy Counter'))

    def test_killed_job_status(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'killed_job_status')
        with open(_stderr) as _file:
            _stdout = "\n".join(_file.readlines())
            job = JobStatus("test_mr_job")
            job._parse_stdout_(stream=_stdout)
            self.assertTrue(job.is_killed())
            self.assertEqual(
                None,
                job.failure_reason())
            # counters
            self.assertEqual(0, len(job.counters()), "Error : Counters should not be available for killed job")
            self.assertEqual(None, job.counter(group='Map-Reduce Framework', counter='Map input records'))

    def test_failed_job_status(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'failed_job_status')
        with open(_stderr) as _file:
            job = JobStatus("test_job")
            job._parse_stdout_(stream="\n".join(_file.readlines()))
            self.assertFalse(job.is_running())
            self.assertTrue(job.is_failed())
            self.assertEqual(
                'task 1412153770896_0092_m_000000 failed 9 times '
                'For details check tasktracker at: vm-cluster-node4:8041',
                job.failure_reason())
            self.assertEqual(0, len(job.counters()), "Error : Counters should not be available for failed job")
            self.assertEqual(None, job.counter(group='Map-Reduce Framework', counter='Map input records'))

    def test_running_job_status(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'running_job_status')
        with open(_stderr) as _file:
            job = JobStatus("test_job")
            job._parse_stdout_(stream="\n".join(_file.readlines()))
            self.assertTrue(job.is_running())
            # some counters may be available for running job
            self.assertEqual(1, len(job.counters()), "Error : injected stdout contain 1 counter group")

    def test_processing_status_for_failed_job(self):
        _stderr = os.path.join(os.path.dirname(__file__), 'resources',
                               'mapreduce', 'failed_job_status')
        with open(_stderr) as _file:
            job = JobStatus("test_job")
            job._parse_stdout_(stream="\n".join(_file.readlines()))
            status = job.job_stats
            self.assertEqual("FAILED", status['Job state'])
            self.assertEqual(
                ("task 1412153770896_0092_m_000000 failed"
                 " 9 times For details check tasktracker at:"
                 " vm-cluster-node4:8041"),
                status['reason for failure'])

    def test_command_generation(self):
        JobStatus("test_job", executor=self.mock_executor("hadoop job -status test_job", stdout=""))

    def mock_executor(self, expected_command, status=0, stdout=None, stderr=None):
        def executor(cmd, *args):
            self.assertEqual(expected_command, build_command(cmd, *args))
            result = mock.Mock(spec=Result, status=status, stdout=stdout, stderr=stderr)
            return result

        return executor




