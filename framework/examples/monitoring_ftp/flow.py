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
Monitoring file system on ftp

Flow gets metadata of files on FTP server and on HDFS.
Compares them and get only new files on FTP that don't exist on HDFS.
Download new files to HDFS with partition.
"""
from ConfigParser import RawConfigParser
import os
from merlin.common.logger import get_logger
from merlin.flow.flow import Workflow, FlowRegistry
from merlin.flow.listeners import LoggingListener, WorkflowListener
from merlin.fs.ftp import ftp_client
from merlin.fs.hdfs import HDFS
from merlin.fs.localfs import LocalFS
from merlin.fs.utils import FileUtils
from merlin.tools.hive import Hive

BASE_DIR = "/tmp/base_folder"
log = get_logger("MonitoringFTP")

config = RawConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "resources/ftp_config.ini"))
HOST_DOWNLOAD = config.get("ftp", "host.download")
USER_NAME = config.get("ftp", "user.name")
PASSWORD = config.get("ftp", "password")
PATH = config.get("ftp", "path")


def get_name(path):
    if not hasattr(path, 'name'):
        raise TypeError('FileDescriptor is required. '
                        'Cannot extract file name from {0}'.format(path.__class__))

    return path.name.split("/")[-1]


def parser_partition(path):
    return path.split("/")[-1].split("_")[-2]


def parser_name(path):
    return path.split("/")[-1]


# Gets metadata of files on FTP server
@Workflow.action(flow_name='Flow',
                 action_name='Load file descriptor for files on FTP',
                 on_success='Load file descriptor for files on HDFS',
                 on_error='error')
def load_file_on_ftp(context):
    context['files_on_FTP'] = []
    for ftp_file in ftp_client(
            host=HOST_DOWNLOAD,
            login=USER_NAME,
            password=PASSWORD,
            path=PATH):
        context['files_on_FTP'].append(ftp_file.get_description())


# Gets metadata of files on HDFS.
@Workflow.action(flow_name='Flow',
                 action_name='Load file descriptor for files on HDFS',
                 on_success='Compare two lists',
                 on_error='error')
def load_file_on_hdfs(context):
    _hdfs = HDFS('/tmp/raw')
    context['files_on_HDFS'] = []
    for _file in _hdfs.recursive_list_files():
        if not _file.is_directory():
            context['files_on_HDFS'].append(_file.get_description())


# Compares files on FTP and on HDFS.
@Workflow.action(flow_name='Flow',
                 action_name='Compare two lists',
                 on_success='Load file from FTP to local',
                 on_error='error')
def compare(context):
    context['new_files'] = FileUtils.get_new_files(
        right=context['files_on_HDFS'],
        left=context['files_on_FTP'],
        left_property_extractor=get_name,
        right_property_extractor=get_name)


# Copies only new files on FTP that don't exist on HDFS to local file system.
@Workflow.action(flow_name='Flow',
                 action_name='Load file from FTP to local',
                 on_success='Load file from local to HDFS',
                 on_error='error_load_file_from_ftp_to_local')
def load_file_from_ftp_to_local(context):
    for _file in context['new_files']:
        ftp_client(
            host=HOST_DOWNLOAD,
            login=USER_NAME,
            password=PASSWORD,
            path=_file.name).download_file(local_path=os.path.join(os.path.dirname(__file__),
                                                                   "resources/tmp/"))


# Copies files from local file system to HDFS.
@Workflow.action(flow_name='Flow',
                 action_name='Load file from local to HDFS',
                 on_success='Hive add partition',
                 on_error='error_load_file_from_local_to_hdfs')
def load_file_from_local_to_hdfs(context):
    context['new_pathes'] = []
    for _file in LocalFS(os.path.join(os.path.dirname(__file__), "resources/tmp")):
        HDFS("/tmp/raw/{0}".format(parser_partition(_file.path))) \
            .create(directory=True)
        LocalFS(os.path.join(os.path.dirname(__file__),
                             "resources/tmp/{0}").format(_file.path)) \
            .copy_to_hdfs(hdfs_path="/tmp/raw/{0}/".format(parser_partition(_file.path)))
        context['new_pathes'].append("/tmp/raw/{0}".format(parser_partition(_file.path)))


# Adds partition to Hive's metadata
@Workflow.action(flow_name='Flow',
                 action_name='Hive add partition',
                 on_success='end',
                 on_error='error')
def hive_add_partition(context):
    for path in context['new_pathes']:
        Hive.load_queries_from_string(query="USE hive_monitoring; ALTER TABLE data "
                                            "ADD PARTITION(date='{0}') "
                                            "LOCATION '{1}';"
                                      .format(parser_name(path), path)).run()


@Workflow.action(flow_name='Flow', action_name='error', on_success='end', on_error='end')
def on_flow_failed(context):
    # Logs error
    log.error('handle error : {}'.format(context['exception']))


# Clean resources on local file system after error
@Workflow.action(flow_name='Flow', action_name='error_load_file_from_ftp_to_local', on_success='end', on_error='end')
def on_flow_failed(context):
    local_file = LocalFS(path=os.path.join(os.path.dirname(__file__),
                                           'resources/tmp'))
    if local_file.exists():
        local_file.delete_directory()


# Clean resources on HDFS file system after error
@Workflow.action(flow_name='Flow', action_name='error_load_file_from_local_to_hdfs', on_success='end', on_error='end')
def on_flow_failed(context):
    hdfs_file = HDFS("{0}/raw".format(BASE_DIR))
    if hdfs_file.exists():
        hdfs_file.delete(recursive=True)


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
        if action_name == "load_file_from_local_to_hdfs":
            file = open('resources/step', 'w')
            file.write(action_name)
            file.close()


if __name__ == '__main__':
    step = 'Load file descriptor for files on FTP'

    # Checks if file with last failed step is exists
    # and reads this step
    if os.path.isfile('resources/step'):
        file = open('resources/step', 'r')
        step = file.read()
        file.close()

    flow = FlowRegistry.flow('Flow')

    # Runs flow
    _context = flow.run(action=step,
                        listeners=[LoggingListener("Flow"), WorkflowFailOverController()])