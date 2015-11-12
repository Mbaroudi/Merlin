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
A lightweight workflow routing engine

"""
from contextlib import contextmanager
import traceback
from merlin.common.logger import get_logger


class FlowRegistry(object):
    """
    Registry of reusable workflows
    """
    # workflow task registry
    __REGISTRY__ = {}

    @staticmethod
    def flow(name, create_new_flow=False):
        """
        Fetch specific workflow from registry
        :param name:  flow name
        :param create_new_flow: if True will create new workflow in case registry
            does not contain workflow with specified name
        :return: workflow with specified name
        :raise: WorkflowError in case registry does not contain workflow with specified name
        """
        if name in FlowRegistry.__REGISTRY__:
            return FlowRegistry.__REGISTRY__[name]
        elif create_new_flow:
            FlowRegistry.__REGISTRY__[name] = Workflow(name)
            return FlowRegistry.__REGISTRY__[name]
        else:
            raise WorkflowError("Unknown flow %s " % name)

    @staticmethod
    def clear():
        """Removes all workflows from registry"""
        FlowRegistry.__REGISTRY__ = {}


class Workflow(object):
    """ Workflow definition"""
    def __init__(self, name):
        super(Workflow, self).__init__()
        self.name = name
        self.__action_registry__ = {}
        self.log = get_logger(self.name)

    @staticmethod
    def action(flow_name, action_name, on_success, on_error):
        """
        Annotation which can be used to associate action with specific workflow
        and define next steps and error handlers.
        Any python functions which accepts only one argument can be marked as workflow action

        :param flow_name: name of the workflow
        :param action_name: identifier of the action to be created
        :param on_success: name of rhe action to be called next
        :param on_error: me of the action to be called in case exception happens
        :return:
        """
        def _function(func):
            """adds  new action to workflow"""
            _flow = FlowRegistry.flow(name=flow_name, create_new_flow=True)
            _flow.add_action(action_name=action_name,
                             on_success=on_success,
                             on_error=on_error,
                             action=func)

        return _function

    def add_action(self, action_name, action, on_success, on_error):
        """
        Creates and adds new action (step) to specific workflow
        :param action_name: identifier of the action to be created
        :param action: action function pointer
        :param on_success: name of rhe action to be called next
        :param on_error: name of the action to be called in case exception happens
        :return:
        """
        if action_name not in self.__action_registry__:
            _workflow_action = WorkflowAction(action_name, on_success, on_error)
            self.__action_registry__[action_name] = _workflow_action
            _workflow_action.run = action

    def run(self, action, context=None, listeners=None):
        """
        Invokes a workflow
        :param action: workflow action to be called
        :param context: workflow shared context
        :param listeners: listeners to be attached to this workflow
        :return: workflow shared context
        :raise:
        """
        if context is None:
            context = {}
        if listeners is None:
            listeners = []
        if action in self.__action_registry__:
            step = self.__action_registry__[action]
            try:
                with self.__event_dispatcher__(step, listeners):
                    step.run(context)
            except FatalWorkflowError as ex:
                raise ex
            except Exception as ex:
                context['exception'] = ex
                context['exception.stacktrace'] = str(traceback.extract_stack())
                self.run(step.on_error, context, listeners)
            else:
                self.run(step.on_success, context, listeners)
        else:
            self.log.warn("Action '{}' is not defined. Workflow will be terminated".format(action))

        return context

    @contextmanager
    def __event_dispatcher__(self, action, listeners):
        try:
            self.__foreach__(lambda l: l.on_begin(action.name), listeners)
            yield
            self.__foreach__(lambda l: l.on_complete(action.name), listeners)
        except Exception as ex:
            self.__foreach__(lambda l: l.on_error(action.name, ex), listeners)
            raise ex

    def __foreach__(self, function, seq):
        for item in seq:
            function(item)


class WorkflowAction(object):
    """Workflow step definition"""

    def __init__(self, name, on_success, on_error):
        """
        Creates new workflow action
        :param name: action identifier. should be unique within parent workflow
        :param on_success: name of rhe action to be called next
        :param on_error: name of the action to be called
            in case this action fails
        """
        super(WorkflowAction, self).__init__()
        self.name = name
        self.on_success = on_success
        self.on_error = on_error

    def run(self, context):
        """
        Causes this action to begin execution
        :param context: workflow context. Can be used for passing values between tasks
        """
        pass


class WorkflowError(Exception):
    """General workflow error."""
    pass


class FatalWorkflowError(WorkflowError):
    """General workflow error."""
    pass


