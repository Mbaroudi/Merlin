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
WebHCatalog Client.

This client provides Python wrapper for WebHCatalog resources:
    ddl/database/:db/table/:table/property (GET) - List table properties.

    ddl/database/:db/table/:table/property/:property (GET) - Return the value of a single table property.

    ddl/database/:db/table/:table/property/:property (PUT) - Set a table property.

"""

import requests
from merlin.common.metastores import MetaStore
from merlin.common.logger import get_logger


class WebHCatalog():
    """
    WebHCatalog Client
    """
    URL = "http://{host}/templeton/v1/ddl/database/{database}/table/{table}/property/{property}?user.name={username}"
    DATA = "{{ \"value\": \"{value}\" }}"
    HEADERS = {'Content-type': 'application/json'}

    LOG = get_logger("WebHCatalog")

    def __init__(self, username, host="localhost", port=None):
        self.host = "{0}:{1}".format(host, port) if port else host
        self.username = username

    def table_properties(self, table, database="default"):
        """
        Returns TableProperties object
        """
        return TableProperties(database=database, table=table, webhcat=self)


class TableProperties(MetaStore):
    """
    Wrapper on requests module. Has table properties in JSON format
    Implements MetaStore based on WebHCat properties
    """

    def __init__(self, database, table, webhcat):
        MetaStore.__init__(self)
        self.table = table
        self.database = database
        self._values = None
        self._webhcat = webhcat

    def get_property(self, name=None):
        """
        Gets property value by name if property exists else None
        :param name:
        :return:
        """
        if not self._values:
            self._values = self.__get()
        return self._values[name] if name in self._values else None

    def properties_as_map(self):
        """
        Gets all properties in JSON format
        :return:
        """
        if not self._values:
            self._values = self.__get()
        return self._values

    def set_property(self, name, value=""):
        """
        Sets new property for the table
        :param name:
        :param value:
        :return:
        """
        return self.__put(name=name, value=value)

    def _set(self, section, key, value):
        """
        Sets the given option to the specified value.
        :param section:
        :param key:
        :param value:
        :return:
        """
        self.properties_as_map()
        if section:
            self._values["{0}.{1}".format(section, key)] = value
        else:
            self._values[key] = value

    def _has_option(self, section, key):
        """
        If the given section exists, and contains the given option, return True;
        if the given option exists return True;
        otherwise return False.
        :param section:
        :param key:
        :return:
        """
        self.properties_as_map()
        if "{0}.{1}".format(section, key) in self._values:
            return True
        else:
            return key in self._values

    def _get(self, section, key):
        """
        Gets an option value.
        :param section:
        :param key:
        :return:
        """
        self.properties_as_map()
        if "{0}.{1}".format(section, key) in self._values:
            return self._values["{0}.{1}".format(section, key)]
        else:
            return self._values[key]

    def _options(self, section):
        """
        Returns a list of options available in the specified section.
        :param section:
        :return:
        """
        self.properties_as_map()
        _list = []
        section += "."
        for key in self._values:
            if key.startswith(section):
                _list.append(key.split(section)[1])
        return _list

    def __getattr__(self, name):
        return self.get_property(name)

    def __getitem__(self, item):
        return self.get_property(item)

    def __get(self):
        return requests.get(WebHCatalog.URL.format(host=self._webhcat.host,
                                                   database=self.database,
                                                   table=self.table,
                                                   property="",
                                                   username=self._webhcat.username)).json()

    def __put(self, name, value):
        response = requests.put(WebHCatalog.URL.format(host=self._webhcat.host,
                                                       database=self.database,
                                                       table=self.table,
                                                       property=name,
                                                       username=self._webhcat.username),
                                data=WebHCatalog.DATA.format(value=value),
                                headers=WebHCatalog.HEADERS)
        self._values = self.__get()
        return False if 'error' in response.json() else True
