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

    hdfs_file = HDFS("{0}/raw".format(BASE_DIR))
    if hdfs_file.exists():
        hdfs_file.delete(recursive=True)

    config = RawConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "resources/ftp_config.ini"))
    host_download = config.get("ftp", "host.download")
    user_name = config.get("ftp", "user.name")
    password = config.get("ftp", "password")
    path = config.get("ftp", "path")
    ftp = ftp_client(host=host_download,
                     login=user_name,
                     password=password,
                     path="/tmp")

    if ftp.exists():
        ftp.delete(recursive=True)

    local_file = LocalFS(path=os.path.join(os.path.dirname(__file__),
                                           'resources/tmp'))
    if local_file.exists():
        local_file.delete_directory()

    hive = Hive.load_queries_from_string(query="DROP DATABASE IF EXISTS hive_monitoring CASCADE;")
    hive.run()


