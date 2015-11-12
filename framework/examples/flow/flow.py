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
Standard scenario of ETL process.

Flow imports data from mysql to HDFS, process it and upload processed data back to mysql.
"""

import os
from merlin.common.exceptions import MapReduceJobException
from merlin.common.logger import get_logger
from merlin.flow.flow import FlowRegistry, Workflow
from merlin.flow.listeners import WorkflowListener, LoggingListener
from merlin.tools.mapreduce import MapReduce
from merlin.tools.sqoop import Sqoop


BASE_DIR = "/tmp"
LOG = get_logger("SimpleETLFlow")


# Imports data from mysql's table 'test_example.first_table_name'(id,name,count)
# to HDFS's folder '/tmp/data_from_import' in "'id','name','count'" format.
@Workflow.action(flow_name='Flow',
                 action_name='Sqoop import etl step',
                 on_success='MapReduce job etl step',
                 on_error='error')
def load_data_from_rdbms_to_hdfs(context):
    # configure Sqoop import job
    _sqoop_import_job_ = Sqoop.import_data().from_rdbms(
        host="127.0.0.1",
        rdbms="mysql",
        database="test_example",
        username="root",
        password_file="{0}/rdbms.password".format(BASE_DIR)
    ).table(
        table="first_table_name"
    ).to_hdfs(target_dir="{0}/data_from_import".format(BASE_DIR))
    _sqoop_import_job_.run()


# Counts words:
#         - Reads files from folder '/tmp/data_from_import';
#         - Splits stroke on mapper and gives to reducers in format (new Text('name'), new IntWritable('count'));
#         - Summarizes on reducer and return in format (new Text('name'), new IntWritable('count'));
#         - Writes files to folder '/tmp/data_to_export'.
@Workflow.action(flow_name='Flow',
                 action_name='MapReduce job etl step',
                 on_success='Sqoop export etl step',
                 on_error='error')
def process_data(context):
    # Configure and run MapReduce job
    _mapreduce_job_ = MapReduce.prepare_mapreduce_job(
        jar=os.path.join(os.path.dirname(__file__), 'resources/WordsCount-1.0-SNAPSHOT.jar'),
        main_class="WordsCountJob",
        name="MAPREDUCE_Counting"
    ).with_config_option("input", "{0}/data_from_import".format(BASE_DIR)) \
        .with_config_option("output", "{0}/data_to_export".format(BASE_DIR))
    _mapreduce_job_.run()
    status = _mapreduce_job_.status()
    if not status.is_succeeded():
        raise MapReduceJobException("MapReduce job failed: {}".format(
            status.failure_reason() if status.is_failed() else 'NONE'))


# Exports data to mysql's table 'test_example.second_table_name'(id,name,count)
# from HDFS's folder '/tmp/data_to_export'.
@Workflow.action(flow_name='Flow',
                 action_name='Sqoop export etl step',
                 on_success='end',
                 on_error='error')
def load_data_from_hdfs_to_rdbms(context):
    # Configure and run Sqoop export job
    _sqoop_export_job_ = Sqoop.export_data().to_rdbms(
        host="127.0.0.1",
        rdbms="mysql",
        database="test_example",
        username="root",
        password_file="{0}/rdbms.password".format(BASE_DIR)
    ).table(
        table="second_table_name",
        columns=["name", "count"]
    ).from_hdfs(
        export_dir="{0}/data_to_export".format(BASE_DIR)
    )
    _sqoop_export_job_.run()


@Workflow.action(flow_name='Flow', action_name='error', on_success='end', on_error='end')
def on_flow_failed(context):
    # Logs error
    LOG.error('handle error : {0}'.format(context['exception']))


# Creates, deletes or updates temporary file
# that contains name of last failed step
#
# If workflow step fails then file with this step will be created
# Next run of workflow will start from this failed step
#
# File will be deleted in the next run of workflow before the step runs
class WorkflowFailOverController(WorkflowListener):

    def __init__(self, name='WorkflowFailOverController'):
        self.log = get_logger(name)
        self.metrics = {}

    def on_begin(self, action_name):
        if 'error' not in action_name:
            if os.path.isfile('resources/step'):
                os.remove('resources/step')

    def on_error(self, action_name, exception):
        file = open('resources/step', 'w')
        file.write(action_name)
        file.close()


if __name__ == '__main__':
    step = 'Sqoop import etl step'

    # Checks if file with last failed step is exists
    # and reads this step
    if os.path.isfile('resources/step'):
        file = open('resources/step', 'r')
        step = file.read()
        file.close()

    flow = FlowRegistry.flow('Flow')

    # Run flow
    _context = flow.run(action=step,
                        listeners=[LoggingListener("Flow"), WorkflowFailOverController()])