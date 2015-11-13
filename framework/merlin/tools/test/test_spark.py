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

from unittest2 import TestCase
from merlin.common.configurations import Configuration
from merlin.common.metastores import IniFileMetaStore
from merlin.common.test_utils import mock_executor
from merlin.tools.spark import SparkApplication, SparkMaster


class TestSpark(TestCase):
    def test_spark_submit_command_generation(self):
        _command = "spark-submit " \
                   "--master local[10] " \
                   "--class test.SparkApp " \
                   "--name test_app " \
                   "--jars lib001.jar,lib002.jar,lib003.jar " \
                   "--files dim001.cache.txt,dim002.cache.txt " \
                   "--properties-file spark.app.configs " \
                   "--conf \"spark.app.name=test_app spark.executor.memory=512m\" " \
                   "application.jar " \
                   "10"

        spark = SparkApplication(executor=mock_executor(expected_command=_command)) \
            .master(SparkMaster.local(10)) \
            .application(application_jar='application.jar', app_name="test_app", main_class="test.SparkApp") \
            .classpath("lib001.jar", "lib002.jar").classpath("lib003.jar") \
            .add_files("dim001.cache.txt") \
            .add_files("dim002.cache.txt") \
            .config_file(path="spark.app.configs") \
            .with_config_option("spark.app.name", "test_app") \
            .with_config_option("spark.executor.memory", "512m")
        spark.run(10)

    def test_spark_submit_from_ini(self):
        _command = "spark-submit " \
                   "--master local[10] " \
                   "--class test.SparkApp " \
                   "--name test_app " \
                   "--jars lib001.jar,lib002.jar,lib003.jar " \
                   "--files dim001.cache.txt,dim002.cache.txt " \
                   "--properties-file spark.app.configs " \
                   "--conf \"spark.app.name=test_app spark.executor.memory=512m " \
                   "spark.serializer=org.apache.spark.serializer.KryoSerializer\" " \
                   "application.jar " \
                   "10 test"
        metastore=IniFileMetaStore(file=os.path.join(os.path.dirname(__file__), "resources", "spark", "spark.app.ini"))
        spark = SparkApplication.load_preconfigured_job(
            config=Configuration.load(metastore,
                                       readonly=False),
            name="test_spark_app",
            executor=mock_executor(expected_command=_command)).application_jar("application.jar")
        spark.run(10, "test")