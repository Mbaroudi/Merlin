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
Configuration object for reading configs from metastore

"""

import re
from merlin.common.exceptions import ConfigurationError
from merlin.common.metastores import IniFileMetaStore


class Configuration(object):
    """Provides access to configuration parameters."""

    @staticmethod
    def load(metastore, readonly=True, accepts_nulls=True):
        """
        Loads configuration from metastore that should implements MetaStore interface.
        :param readonly: Boolean flag. Indicates if configuration is mutable.
        :param accepts_nulls: Boolean flag. Indicates if configuration accepts nones as a values.
        :return:
        :rtype : Configuration
        """

        return Configuration(metastore, readonly, accepts_nulls)

    @staticmethod
    def create(metastore=IniFileMetaStore(), readonly=False, accepts_nulls=True):
        """
        Creates a new empty configuration based on metastore that should implements MetaStore interface.
        :param readonly: Boolean flag. Indicates if configuration is mutable.
        :param accepts_nulls: Boolean flag. Indicates if configuration accepts nones as a values
        :return:
        :rtype : Configuration
        """
        return Configuration(metastore, readonly, accepts_nulls)

    def __init__(self, metastore, readonly=False, accepts_nulls=True):
        """
        :param metastore: MetaStore implementation
        :param readonly: Boolean flag. Indicates if configuration is mutable.
        :param accepts_nulls: Boolean flag. Indicates if configuration accepts nones as a values
        """
        super(Configuration, self).__init__()
        self._metastore = metastore
        self._readonly = readonly
        self._accepts_nulls = accepts_nulls

    def set(self, section, key, value):
        """
        Updates option value for the named section.
        In case specified section was not found will try to create one
        :param section:  name of the configuration section
        :param key: name of the configuration option
        :param value: value of the configuration option
        :raise: ConfigurationError in case value cannot be added to this configuration:
            - configuration is immutable
            - null values are not allowed

        """
        if self._readonly:
            raise ConfigurationError("Cannot modify readonly context")
        if not self._accepts_nulls and not value:
            raise ConfigurationError("Empty values is not allowed for this configuration")
        self._metastore._set(section, key, value)

    def update_list(self, section, key, *values):
        """
        Updates option value for the named section.
        :param section: name of the configuration section
        :param key: name of the configuration option
        :param values: values of the configuration option
        """
        _list = self.get(section, key) if self.has(section, key) else []
        _list.extend(values)
        self.set(section, key, _list)

    def get(self, section, key):
        """
        Get an option value for the named section.
        :param section: name of the configuration section
        :param key: name of the configuration option
        :return:
        """
        return self.optional(section, key)

    def get_list(self, section, key, delimiter='\n'):
        """
        Get an list of option values for the named section.
        :param section: name of the configuration section
        :param key: name of the configuration option
        :param delimiter: char delimiter which can be used to split string
        :return:
        """
        _values = self.get(section, key)
        if not _values:
            return None
        if isinstance(_values, list):
            return _values
        else:
            return re.split(delimiter, _values.strip())

    def options(self, section):
        """Returns a list of options available in the specified section"""
        return self._metastore._options(section)

    def require(self, section, key):
        """
        Get an required option value for the named section.
        :param section: name of the configuration section
        :param key: name of the configuration option
        :return: value
        :raise: ConfigurationError in case option was not found
        """
        if not self._metastore._has_option(section, key):
            raise ConfigurationError("{0} is required".format(key))
        else:
            return self._metastore._get(section, key)

    def require_list(self, section, key, delimiter='\n'):
        """
       Get an required option value for the named section.
       :param section: name of the configuration section
       :param key: name of the configuration option
       :return: value
       :raise: ConfigurationError in case option was not found
       """
        _values = self.require(section, key)
        if isinstance(_values, list):
            return _values
        else:
            return [_values] if delimiter not in _values else re.split(delimiter, _values.strip())

    def has(self, section, key):
        """Check for the existence of a given option in a given section."""
        return self._metastore._has_option(section, key)

    def optional(self, section, key, default=None):
        """Get an option value for the named section.
        Or returns default value if option was not found """
        return self._metastore._get(section, key) if self.has(section, key) else default

    def __str__(self):
        return str(self._metastore.__dict__)



