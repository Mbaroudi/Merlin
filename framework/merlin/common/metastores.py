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

import ConfigParser
from abc import abstractmethod


class MetaStore():
    """
    Interface for metastore in merlin.common.configurations.Configuration class
    """
    def __init__(self):
        pass

    @abstractmethod
    def _set(self, section, key, value):
        """
        Sets value by given section and key
        :param section:
        :param key:
        :param value:
        :return:
        """
        pass

    @abstractmethod
    def _options(self, section):
        """
        Returns list of keys by given section
        :param section:
        :return:
        """
        pass

    @abstractmethod
    def _has_option(self, section, key):
        """
        Returns True if given section and key exists
        :param section:
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def _get(self, section, key):
        """
        Gets value by given section and key
        :param section:
        :param key:
        :return:
        """
        pass


class IniFileMetaStore(MetaStore):
    """
    Implements MetaStore based on INI file.
    Configuration file structure is similar to Microsoft Windows INI files.
    The configuration file consists of sections,
    led by a [section] header and followed by name= value entries.
    Lines beginning with '#' or ';' are ignored and may be used to provide comments.

    Supports interpolation: values can contain format strings which refer
    to other values in the same section, or values in a special DEFAULT section
    """

    def __init__(self, file=None):
        MetaStore.__init__(self)
        config = ConfigParser.ConfigParser()
        if file:
            config.read(file)
        self._config = config

    def _set(self, section, key, value):
        """
        Sets value by given section and key
        :param section:
        :param key:
        :param value:
        :return:
        """
        if not self._config.has_section(section):
            self._config.add_section(section)
        self._config.set(section, key, value)

    def _options(self, section):
        """
        Returns list of keys by given section
        :param section:
        :return:
        """
        return self._config.options(section)

    def _has_option(self, section, key):
        """
        Returns True if given section and key exists
        :param section:
        :param key:
        :return:
        """
        return self._config.has_option(section, key)

    def _get(self, section, key):
        """
        Gets value by given section and key
        :param section:
        :param key:
        :return:
        """
        return self._config.get(section, key)
