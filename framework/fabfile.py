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

#!/usr/bin/env python
import os
from ConfigParser import ConfigParser
import socket
from fabric.api import local, env, put, run, cd

DEPLOY_CONFIG_FILE_NAME = "deploy.config"


def profile(prof):
    """
        Sets build profile.

    :param prof: profile directory. For example: dev, prod, qa
    :type prof: string
    """
    config_file = os.path.join("profiles", prof, DEPLOY_CONFIG_FILE_NAME)
    global config

    config = ConfigParser()
    config.read(config_file)
    env.username = config.get("deployment", "deployment.user")
    env.password = config.get("deployment", "deployment.password")
    env.user_home = config.get("deployment", "deployment.user_home")
    env.deployment_dir = config.get("deployment", "deployment.dir")
    env.test_resources = config.get("deployment", "test.resources")

    _localhost = socket.gethostname()
    env.hosts.extend(
        ["%s@%s" % (env.username, host) for host in config.get("deployment", "deployment.hosts").split("\n")])
    print ",".join(env.hosts)


def pack():
    # create a new source distribution as tarball
    local('python setup.py sdist --formats=gztar', capture=False)


def install():
    with cd(env.deployment_dir):
        local('virtualenv .')
        local('source bin/activate')
        local('python setup.py install')


def deploy():
    dist = local('python setup.py --fullname', capture=True).strip()
    print "Deploying", dist


def _deploy_test_streaming_job_resources_():
    def _put(path, mode=0755):
        os.chmod(path, mode)
        _dst = os.path.join(env.test_resources, "streaming_mapreduce")
        run("mkdir -p {dr}".format(dr=_dst))
        put(path, _dst, mirror_local_mode=True)

    _mapper = os.path.join(env.user_home, "framework", "merlin", "tools", "test", "resources", "mapreduce",
                           "mapper.py")
    _reducer = os.path.join(env.user_home, "framework", "merlin", "tools", "test", "resources", "mapreduce",
                            "reducer.py")
    _put(_mapper)
    _put(_reducer)


def test(test_case=""):
    _deploy_test_streaming_job_resources_()


if __name__ == '__main__':
    print "YAHOOOO"
