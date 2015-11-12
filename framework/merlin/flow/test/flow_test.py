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

import uuid

import mock
from unittest2 import TestCase, expectedFailure

from merlin.flow.flow import FlowRegistry, WorkflowError, Workflow, FatalWorkflowError


class FlowRegistryTest(TestCase):
    def test_should_create_new_flow(self):
        flow_name = "flow %s" % str(uuid.uuid4())
        FlowRegistry.flow(flow_name, True)
        self.assertIsNotNone(FlowRegistry.flow(flow_name, False), 'new flow was not created')

    def test_should_raise_exception_if_flow_not_found(self):
        flow_name = "flow %s" % str(uuid.uuid4())
        self.assertRaises(WorkflowError, FlowRegistry.flow, flow_name, False)

    @expectedFailure
    def test_should_not_raise_exception_if_creates_flow(self):
        flow_name = "flow %s" % str(uuid.uuid4())
        self.assertRaises(WorkflowError, FlowRegistry.flow, flow_name, True)


class WorkflowTest(TestCase):
    def test_flow_action_registration(self):
        step_id = "step_%s" % str(uuid.uuid4())

        @Workflow.action(flow_name='test_flow_action_registration', action_name=step_id, on_success='second step',
                         on_error='error')
        def test_action(context):
            pass

        flow = FlowRegistry.flow('test_flow_action_registration', False)
        self.assertTrue(step_id in flow.__action_registry__)

    def test_flow_routing(self):
        flow_id = 'test_flow_routing'
        first_step_id = 'step_001_%s' % str(uuid.uuid4())
        second_step_id = 'step_002_%s' % str(uuid.uuid4())

        @Workflow.action(flow_name=flow_id,
                         action_name=first_step_id,
                         on_success=second_step_id,
                         on_error='error')
        def flow_routing_first_action(context):
            self.assertFalse(first_step_id in context)
            self.assertFalse(second_step_id in context)
            context[first_step_id] = True

        @Workflow.action(flow_name=flow_id,
                         action_name=second_step_id,
                         on_success='end',
                         on_error='error')
        def flow_routing_second_action(context):
            self.assertTrue(first_step_id in context)
            self.assertFalse(second_step_id in context)
            context[second_step_id] = True

        @Workflow.action(flow_name=flow_id,
                         action_name='error',
                         on_success='end',
                         on_error='end')
        def flow_routing_error_handler(context):
            raise FatalWorkflowError()

        flow = FlowRegistry.flow(flow_id, False)
        self.assertTrue(first_step_id in flow.__action_registry__)
        self.assertTrue(second_step_id in flow.__action_registry__)
        self.assertTrue('error' in flow.__action_registry__)
        _context = flow.run(first_step_id)

        self.assertTrue(first_step_id in _context)
        self.assertTrue(second_step_id in _context)

    def test_flow_routing_with_checked_exception(self):
        flow_id = 'test_flow_routing_with_checked_exception'

        first_step_id = 'step_001_%s' % str(uuid.uuid4())
        second_step_id = 'step_002_%s' % str(uuid.uuid4())
        error_handler_id = 'error_handler_%s' % str(uuid.uuid4())

        @Workflow.action(flow_name=flow_id,
                         action_name=first_step_id,
                         on_success=second_step_id,
                         on_error=error_handler_id)
        def flow_routing_with_checked_exception_first_action(context):
            self.assertFalse(first_step_id in context)
            self.assertFalse(second_step_id in context)
            self.assertFalse(error_handler_id in context)
            context[first_step_id] = True

        @Workflow.action(flow_name=flow_id,
                         action_name=second_step_id,
                         on_success='end',
                         on_error=error_handler_id)
        def flow_routing_with_checked_exception_second_action(context):
            self.assertTrue(first_step_id in context)
            self.assertFalse(second_step_id in context)
            self.assertFalse(error_handler_id in context)
            context[second_step_id] = True
            raise Exception("excpected")

        @Workflow.action(flow_name=flow_id,
                         action_name=error_handler_id,
                         on_success='end',
                         on_error='end')
        def flow_routing_with_checked_exception_error_handler(context):
            self.assertTrue(first_step_id in context)
            self.assertTrue(second_step_id in context)
            self.assertFalse(error_handler_id in context)
            context[error_handler_id] = True

        flow = FlowRegistry.flow(flow_id, False)
        self.assertTrue(first_step_id in flow.__action_registry__)
        self.assertTrue(second_step_id in flow.__action_registry__)
        self.assertTrue(error_handler_id in flow.__action_registry__)
        _context = flow.run(first_step_id)

        self.assertTrue(first_step_id in _context)
        self.assertTrue(second_step_id in _context)
        self.assertTrue(error_handler_id in _context)

    def test_flow_routing_with_unchecked_exception(self):
        flow_id = 'test_flow_routing_with_unchecked_exception'
        first_step_id = 'step_001_%s' % str(uuid.uuid4())
        second_step_id = 'step_002_%s' % str(uuid.uuid4())
        error_handler_id = 'error_handler_%s' % str(uuid.uuid4())


        @Workflow.action(flow_name=flow_id,
                         action_name=first_step_id,
                         on_success=second_step_id,
                         on_error=error_handler_id)
        def first_action(context):
            self.assertFalse(first_step_id in context)
            self.assertFalse(second_step_id in context)
            self.assertFalse(error_handler_id in context)
            context[first_step_id] = True
            raise FatalWorkflowError("this flow should fail")

        @Workflow.action(flow_name=flow_id,
                         action_name=second_step_id,
                         on_success='end',
                         on_error=error_handler_id)
        def second_action(context):
            self.fail("this action should never be called")

        @Workflow.action(flow_name=flow_id,
                         action_name=error_handler_id,
                         on_success='end',
                         on_error='end')
        def error_handler(context):
            self.fail("error handler should not be called for FatalWorkflowError")

        flow = FlowRegistry.flow(flow_id, False)
        self.assertTrue(first_step_id in flow.__action_registry__)
        self.assertTrue(second_step_id in flow.__action_registry__)
        self.assertTrue(error_handler_id in flow.__action_registry__)

        _context = {}

        self.assertRaises(FatalWorkflowError, flow.run, first_step_id, context=_context)
        self.assertTrue(first_step_id in _context)
        self.assertFalse(second_step_id in _context)
        self.assertFalse(error_handler_id in _context)

    def test_event_dispatcher(self):
        flow_id = 'test_event_dispatcher'
        first_step_id = 'step_001_%s' % str(uuid.uuid4())
        second_step_id = 'step_002_%s' % str(uuid.uuid4())
        error_handler_id = 'error_handler_%s' % str(uuid.uuid4())

        @Workflow.action(flow_name=flow_id,
                         action_name=first_step_id,
                         on_success=second_step_id,
                         on_error=error_handler_id)
        def flow_routing_first_action(context):
            pass

        flow = FlowRegistry.flow(flow_id, False)
        self.assertTrue(first_step_id in flow.__action_registry__)
        listener = mock.MagicMock()
        _context = flow.run(first_step_id, listeners=[listener])
        listener.on_begin.assert_called_with(first_step_id)
        listener.on_complete.assert_called_with(first_step_id)

    def test_error_notifier(self):
        flow_id = 'test_event_dispatcher'
        first_step_id = 'step_001_%s' % str(uuid.uuid4())
        second_step_id = 'step_002_%s' % str(uuid.uuid4())
        error_handler_id = 'error_handler_%s' % str(uuid.uuid4())

        exception = FatalWorkflowError()

        @Workflow.action(flow_name=flow_id,
                         action_name=first_step_id,
                         on_success=second_step_id,
                         on_error=error_handler_id)
        def flow_routing_first_action(context):
            raise exception

        flow = FlowRegistry.flow(flow_id, False)
        self.assertTrue(first_step_id in flow.__action_registry__)
        listener = mock.MagicMock()
        try:
            _context = flow.run(first_step_id, listeners=[listener])
        except FatalWorkflowError as ex:
            pass
        listener.on_begin.assert_called_with(first_step_id)
        listener.on_error.assert_called_with(first_step_id, exception)