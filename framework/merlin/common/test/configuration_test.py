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
from merlin.common.exceptions import ConfigurationError
from merlin.common.metastores import IniFileMetaStore


class ConfigurationTest(TestCase):
    def test_load_config_from_file(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        _section = 'section_a'
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore)
        self.assertTrue(_config.has(section=_section, key='key'),
                        'Cannot find "key" option in test config')
        self.assertEqual('value', _config.require(_section, 'key'))

    def test_should_not_be_able_to_add_new_items(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=True, accepts_nulls=False)
        self.assertRaises(
            excClass=ConfigurationError,
            callableObj=_config.set,
            section='test',
            key='key',
            value='value')

    def test_should_be_able_to_add_new_items(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        _config.set(section='section_a', key='new_key', value='new_value')
        self.assertEqual('value', _config.get('section_a', 'key'), "Can't find old item")
        self.assertEqual('new_value', _config.get('section_a', 'new_key'), "New Item was not added")

    def test_should_be_able_to_add_new_items_to_new_section(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        _config.set(section='section_b', key='new_key', value='new_value')
        self.assertEqual('value', _config.get('section_a', 'key'), "Can't find old item")
        self.assertEqual('new_value', _config.get('section_b', 'new_key'), "Can't find old item")

    def test_should_be_able_to_add_nones(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        _config.set(section='section_a', key='new_key', value=None)
        self.assertEqual('value', _config.get('section_a', 'key'), "Can't find old item")
        self.assertTrue(_config.has('section_a', 'new_key'), "New Item was not added")

    def test_should_not_be_able_to_add_nones(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=False)
        self.assertRaises(ConfigurationError, _config.set, section='section_a', key='new_key', value=None)

    def test_should_be_able_to_add_multiple_values_for_a_single_key(self):
        _values = [1, 2, 3, 4]
        _increment = ['one', 'two', 'three']
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        _config.update_list("section_b", 'list', *_values)
        self.assertListEqual(_values, _config.get_list("section_b", 'list'))
        _config.update_list("section_b", 'list', *_increment)
        self.assertListEqual(_values + _increment, _config.get_list("section_b", 'list'))

    def test_should_be_able_to_split_string_to_multiple_values(self):
        _values = ['one', 'two', 'three']
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore, readonly=False, accepts_nulls=True)
        _config.set("section_b", 'list', ",".join(_values))
        self.assertListEqual(_values, _config.get_list("section_b", 'list', delimiter=','))

    def test_should_raise_exception_if_required_option_was_not_found(self):
        _config_file = os.path.join(os.path.dirname(__file__), 'resources', 'test.ini')
        metastore = IniFileMetaStore(file=_config_file)
        _config = Configuration.load(metastore)
        self.assertRaises(ConfigurationError, _config.require, 'section_a', 'item_a')

    def test_create_new_config(self):
        _config = Configuration.create()
        _section = 'new_section'
        _key = 'new_key'
        _value = 'new_value'
        _config.set(section=_section, key=_key, value=_value)
        self.assertTrue(_config.has(_section, _key), "Config option was not added")
        self.assertEqual(_value, _config.get(_section, _key))



