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
Defines a set of classes and methods that perform common,
often re-used functions.

"""


class ListIterator(object):
    """
    An iterator over a collection.
    """

    def __init__(self, lst):
        """
        :param lst: iterable collection
        """
        self.lst = lst
        self.i = -1

    def __iter__(self):
        return self

    def next(self):
        """
        Returns the next element in the iteration.

        :return: the next element in the iteration
        :raise:  StopIteration if the iteration has no more elements
        """
        if self.i < len(self.lst) - 1:
            self.i += 1
            return self.lst[self.i]
        else:
            raise StopIteration


class ListUtility(object):
    """
    ListUtility
    """

    @staticmethod
    def to_dict(items, key_extractor=None):
        """
        Converts list to a dict
        :type items: list
        :param key_extractor:
        :rtype: dict
        """
        return dict([(item if not key_extractor else key_extractor(item), item) for item in items]) if items else {}

    @staticmethod
    def diff(left, right, left_property_extractor=None, right_property_extractor=None):
        """
        Return list items that exist in left list and don't exist in right list
        :type left: list
        :type right: list
        :type left_property_extractor:
        :type right_property_extractor:
        :rtype: list
        """
        _left = ListUtility.to_dict(left, left_property_extractor)
        _right = ListUtility.to_dict(right, right_property_extractor)
        return (dict((item, _left[item]) for item in _left.keys() if not _right.has_key(item))).values()

    @staticmethod
    def intersect(left, right, left_property_extractor=None, right_property_extractor=None):
        """
        Return list items that exist in left list and exist in right list
        :type left: list
        :type right: list
        :type left_property_extractor:
        :type right_property_extractor:
        :rtype: list
        """
        _left = ListUtility.to_dict(left, left_property_extractor)
        _right = ListUtility.to_dict(right, right_property_extractor)
        return (dict((item, _left[item]) for item in _left.keys() if _right.has_key(item))).values()

    @staticmethod
    def to_string(list_data):
        """
        Converts list to a string with comma-separated value
        :type list_data: list
        :rtype: str
        """
        return ",".join(list_data)


