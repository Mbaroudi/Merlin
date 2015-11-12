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

from mock import MagicMock
from merlin.tools.webhcat import WebHCatalog, TableProperties
import unittest2
import json


class TestWebHCatalog(unittest2.TestCase):
    def test_get_property(self):
        _command = json.loads("{\"message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        self.assertEquals(tbl_prop.get_property("message"), "Bad credentials")
        self.assertEquals(tbl_prop.message, "Bad credentials")

    def test_has_option(self):
        _command = json.loads("{\"message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        self.assertTrue(tbl_prop._has_option(section="", key="message"))

    def test_options(self):
        _command = json.loads("{\"k.message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        self.assertEquals(tbl_prop._options(section="k"), ["message"])

    def test_set(self):
        _command = json.loads("{\"k.message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        tbl_prop._set(section="mys", key="myk", value="myv")
        self.assertTrue(tbl_prop._has_option("mys", "myk"))

    def test_get(self):
        _command = json.loads("{\"k.message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        self.assertEquals(tbl_prop._get(section="k", key="message"), "Bad credentials")

    def test_put_property(self):
        tbl_prop = TableProperties(database="default",
                                   table="table",
                                   webhcat=WebHCatalog(username="vagrant"))
        tbl_prop._TableProperties__put = MagicMock(return_value=True)
        self.assertTrue(tbl_prop.set_property("message"))
        tbl_prop._TableProperties__put = MagicMock(return_value=False)
        self.assertFalse(tbl_prop.set_property("message"))

    def test_as_map(self):
        _command = json.loads("{\"message\":\"Bad credentials\",\"documentation_url\":\"https://developer.github.com/v3\"}")
        tbl_prop = WebHCatalog(username="vagrant").table_properties(database="default",
                                                                    table="table")
        tbl_prop._TableProperties__get = MagicMock(return_value=_command)
        self.assertEquals(tbl_prop.properties_as_map()["message"], "Bad credentials")
