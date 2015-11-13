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

from merlin.tools.hive import Hive
from ConfigParser import RawConfigParser
from merlin.fs.localfs import LocalFS
from merlin.fs.hdfs import HDFS
from merlin.fs.ftp import ftp_client
import os

BASE_DIR = "/tmp"

if __name__ == "__main__":

    # create empty directory '/tmp/raw' on HDFS
    hdfs_file = HDFS("{0}/raw".format(BASE_DIR))
    if hdfs_file.exists():
        hdfs_file.delete(recursive=True)
    hdfs_file.create(directory=True)

    # create empty directory '/tmp/base_dir' on FTP
    config = RawConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "resources/ftp_config.ini"))
    host_download = config.get("ftp", "host.download")
    user_name = config.get("ftp", "user.name")
    password = config.get("ftp", "password")
    path = config.get("ftp", "path")
    ftp = ftp_client(host=host_download,
                     login=user_name,
                     password=password,
                     path=path)

    if ftp.exists():
        ftp.delete(recursive=True)
    ftp.create(make_dir=True)

    # upload file to directory on FTP
    ftp.upload(local_path=os.path.join(os.path.dirname(__file__), "resources/file_12.11.2014_.txt"))
    ftp.upload(local_path=os.path.join(os.path.dirname(__file__), "resources/file_13.11.2014_.txt"))
    ftp.upload(local_path=os.path.join(os.path.dirname(__file__), "resources/file_14.11.2014_.txt"))

    # upload file to HDFS/ create directories
    hdfs_file = HDFS("{0}/raw/12.11.2014".format(BASE_DIR))
    hdfs_file.create(directory=True)
    local_file = LocalFS(path=os.path.join(os.path.dirname(__file__),
                                           'resources/file_12.11.2014_.txt'))
    local_file.copy_to_hdfs(hdfs_path="{0}/raw/12.11.2014".format(BASE_DIR))

    hdfs_file = HDFS("{0}/raw/13.11.2014".format(BASE_DIR))
    hdfs_file.create(directory=True)
    local_file = LocalFS(path=os.path.join(os.path.dirname(__file__),
                                           'resources/file_13.11.2014_.txt'))
    local_file.copy_to_hdfs(hdfs_path="{0}/raw/13.11.2014".format(BASE_DIR))

    # create empty local directory 'tmp' in folder 'resources'
    local_file = LocalFS(path=os.path.join(os.path.dirname(__file__),
                                           'resources/tmp'))
    if local_file.exists():
        local_file.delete_directory()
    local_file.create(directory=True)

    # create HIVE external table with partition
    hive = Hive.load_queries_from_file(path=os.path.join(os.path.dirname(__file__), "resources/script.hql"))
    hive.run()


