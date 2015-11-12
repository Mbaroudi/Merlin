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

from unittest2 import TestCase, skipUnless
from merlin.common.configurations import Configuration
from merlin.common.test_utils import has_command
from merlin.fs.localfs import LocalFS
from merlin.tools.spark import SparkMaster, SparkApplication, TaskOptions


class TestSparkAppSubmit(TestCase):
    masters = [
        SparkMaster.local(),
        SparkMaster.local(1),
        SparkMaster.yarn_client(),
        SparkMaster.yarn_cluster()
    ]

    input_path = os.path.join(os.path.dirname(__file__), "resources", "spark", "input.txt")

    def _spark_application_template_(self, master):
        return SparkApplication().application(
            application_jar=os.path.join(os.path.dirname(__file__), "resources", "spark", "SparkExample.jar"),
            main_class="example.spark.WordCounter"
        ).master(master)

    def spark_app_config_template(self, master, name=str(uuid.uuid4())):
        _config = Configuration.create()
        _config.set(section=name, key=TaskOptions.SPARK_APP_CONFIG_MASTER, value=master)
        _config.set(section=name, key=TaskOptions.SPARK_APP_CONFIG_APPLICATION_JAR,
                    value=os.path.join(os.path.dirname(__file__), "resources", "spark", "SparkExample.jar"))
        _config.set(section=name, key=TaskOptions.SPARK_APP_CONFIG_MAIN_CLASS, value="example.spark.WordCounter")
        return _config

    @skipUnless(has_command('spark-submit'), "Cannot find spark-submit command-line utility")
    def test_spark_app_submit(self):
        # self.run_test(application=self._spark_application_template_(SparkMaster.local()))
        self._run_(application=self._spark_application_template_(SparkMaster.local(1)))
        # self.run_test(application=self._spark_application_template_(SparkMaster.yarn_cluster()))
        # self.run_test(application=self._spark_application_template_(SparkMaster.yarn_client()))

    @skipUnless(has_command('spark-submit'), "Cannot find spark-submit command-line utility")
    def test_preconfigured_spark_app_submit(self):
        section = str(uuid.uuid4())
        _app_config = self.spark_app_config_template(master=SparkMaster.local(1), name=section)
        self._run_(
            application=SparkApplication(
                config=_app_config,
                name=section)
        )

    def _run_(self, application, test_id=str(uuid.uuid4())):
        basedir = LocalFS(os.path.join("/tmp", "test_spark", test_id))
        try:
            basedir.create_directory()
            _app_input = self.input_path
            _app_output_dir = os.path.join(basedir.path, "output")
            status = application.run('file:' + _app_input, 'file:' + _app_output_dir)
            self.assertTrue(status.is_ok(), status.stderr())
            self.assertTrue(os.path.exists(_app_output_dir), status.stderr())
        finally:
            basedir.delete_directory()


