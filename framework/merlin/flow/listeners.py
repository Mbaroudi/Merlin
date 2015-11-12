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

from datetime import datetime
from merlin.common.logger import get_logger


class WorkflowListener(object):
    """ Workflow event listener    """
    def on_error(self, action_name, exception):
        """
        Fired when the workflow action failed.
        :param action_name:
        :param exception:
        """
        pass

    def on_begin(self, action_name):
        """
        Fired right before the workflow starts action.
        :param action_name:
        """
        pass

    def on_complete(self, action_name):
        """
        Fired when the workflow action was successfully completed.
        :param action_name:
        """
        pass


class LoggingListener(WorkflowListener):

    def __init__(self, name='LoggingListener'):
        self.log = get_logger(name)

    def on_error(self, task, exception):
        self.log.error("'%s' failed : %s" % (task, str(exception)))

    def on_begin(self, task):
        self.log.warn("Task '%s' started" % task)

    def on_complete(self, task):
        self.log.warn("Task '%s' finished" % task)


class ProfilingListener(WorkflowListener):

    def __init__(self, name='ProfilingListener'):
        self.log = get_logger(name)
        self.metrics = {}

    def on_begin(self, task):
        self.metrics[task] = [datetime.now()]

    def on_complete(self, task):
        self.metrics[task].append(datetime.now())
        duration = self.metrics[task][1] - self.metrics[task][0]
        self.metrics[task].append(duration)
        self.log.warn("Task '{0}' started at {1}, finished at {2}. Duration = {3} second(s)"
                      .format(task,
                              self.metrics[task][0].strftime('%Y/%m/%d %H:%M:%S'),
                              self.metrics[task][1].strftime('%Y/%m/%d %H:%M:%S'),
                              self.metrics[task][2].total_seconds()))


