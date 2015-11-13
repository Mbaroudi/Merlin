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
Slowly Changing Dimension(SCD) Type 2 processing

The "Slowly Changing Dimension" problem is a common one particular to data warehousing.
In a nutshell, this applies to cases where the attribute for a record varies over time.
Type 2 slowly changing dimension should be used when it is necessary for the data warehouse
to track historical changes.
This method tracks historical data by creating multiple records for a given natural key
"""
import os
from datetime import datetime
from merlin.common.logger import get_logger
from merlin.flow.flow import Workflow, FlowRegistry
from merlin.flow.listeners import WorkflowListener, LoggingListener
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS
from merlin.tools.pig import Pig


# Uploads files with updated SCD from local file system to HDFS
@Workflow.action(flow_name='Flow',
                 action_name='Copying scd updates to raw area on HDFS',
                 on_success='Pig job. Merge Active snapshot with updates',
                 on_error='error')
def upload_to_hdfs_updates(context):
    LocalFS(_scd_updates).copy_to_hdfs(_hdfs_tmpdir.path)


# Runs Pig job to merge active SCD snapshot with provided updates
@Workflow.action(flow_name='Flow',
                 action_name='Pig job. Merge Active snapshot with updates',
                 on_success='Uploading job result to local fs',
                 on_error='error')
def merge_snapshot_with_updates(context):
    context["partition"] = datetime.now().strftime('%Y%m%d')
    pig_job = Pig.load_commands_from_file(_pig_script) \
        .with_parameter("active_snapshot", _scd_active_snapshot) \
        .with_parameter("data_updates", os.path.join(_hdfs_tmpdir.path, os.path.basename(_scd_updates))) \
        .with_parameter('output', _hdfs_job_output) \
        .with_parameter("date", context["partition"])
    pig_job.run()


# Uploads job result back to Local File System
@Workflow.action(flow_name='Flow',
                 action_name='Uploading job result to local fs',
                 on_success='end',
                 on_error='error')
def upload_result_to_local(context):
    HDFS(_hdfs_job_output).merge("{0}.{1}".format(_scd_updates, context["partition"]))


@Workflow.action(flow_name='Flow', action_name='error', on_success='end', on_error='end')
def on_flow_failed(context):
    # Logs error
    log.error('handle error : {}'.format(context['exception']))


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
    log = get_logger("SCD")

    # Prepare paths
    _pig_script = os.path.join(os.path.dirname(__file__), 'scd_processing.pig')
    _scd_active_snapshot = '/tmp/scd.active/scd.active.csv'
    _scd_updates = os.path.join(os.path.dirname(__file__), 'resources', 'scd.update.csv')
    _hdfs_job_output = '/tmp/scd.updated'

    _local_folder_to_monitor = LocalFS(os.path.join(os.path.dirname(__file__), 'resources'))
    _hdfs_basedir = HDFS('/tmp/scd.active')
    _hdfs_tmpdir = HDFS('/tmp/scd.tmp')
    _hdfs_tmpdir.create_directory()

    if _scd_updates and LocalFS(_scd_updates).exists():

        # Checks if file with last failed step is exists
        # and reads this step
        step = 'Copying scd updates to raw area on HDFS'
        if os.path.isfile('resources/step'):
            file = open('resources/step', 'r')
            step = file.read()
            file.close()

        flow = FlowRegistry.flow('Flow')

        # Runs flow
        _context = flow.run(action=step,
                            listeners=[LoggingListener("Flow"), WorkflowFailOverController()])
    else:
        log.info("Nothing to process")
