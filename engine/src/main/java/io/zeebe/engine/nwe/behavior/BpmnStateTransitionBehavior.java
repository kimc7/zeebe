/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.behavior;

import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.processor.KeyGenerator;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableSequenceFlow;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

public final class BpmnStateTransitionBehavior {

  private final TypedStreamWriter streamWriter;
  private final KeyGenerator keyGenerator;
  private final BpmnStateBehavior stateBehavior;

  public BpmnStateTransitionBehavior(
      final TypedStreamWriter streamWriter,
      final KeyGenerator keyGenerator,
      final BpmnStateBehavior stateBehavior) {
    this.streamWriter = streamWriter;
    this.keyGenerator = keyGenerator;
    this.stateBehavior = stateBehavior;
  }

  public void transitionToActivated(final BpmnElementContext context) {
    transitionTo(context, WorkflowInstanceIntent.ELEMENT_ACTIVATED);

    // TODO (saig0): update state because of the step guards
    stateBehavior.updateElementInstance(
        context,
        elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_ACTIVATED));
  }

  public void transitionToTerminated(final BpmnElementContext context) {
    transitionTo(context, WorkflowInstanceIntent.ELEMENT_TERMINATED);

    stateBehavior.updateElementInstance(
        context,
        elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_TERMINATED));
  }

  public void transitionToCompleting(final BpmnElementContext context) {
    transitionTo(context, WorkflowInstanceIntent.ELEMENT_COMPLETING);

    stateBehavior.updateElementInstance(
        context,
        elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_COMPLETING));
  }

  public void transitionToCompleted(final BpmnElementContext context) {
    transitionTo(context, WorkflowInstanceIntent.ELEMENT_COMPLETED);

    stateBehavior.updateElementInstance(
        context,
        elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_COMPLETED));
  }

  private void transitionTo(final BpmnElementContext context, final WorkflowInstanceIntent intent) {
    streamWriter.appendFollowUpEvent(
        context.getElementInstanceKey(), intent, context.getRecordValue());
  }

  public void takeSequenceFlow(
      final BpmnElementContext context, final ExecutableSequenceFlow sequenceFlow) {

    final var record =
        context
            .getRecordValue()
            .setElementId(sequenceFlow.getId())
            .setBpmnElementType(sequenceFlow.getElementType());

    streamWriter.appendNewEvent(
        keyGenerator.nextKey(), WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN, record);
  }

  public ElementInstance activateChildInstance(
      final BpmnElementContext context, final ExecutableFlowElement childElement) {

    final var childInstanceRecord =
        context
            .getRecordValue()
            .setFlowScopeKey(context.getElementInstanceKey())
            .setElementId(childElement.getId())
            .setBpmnElementType(childElement.getElementType());

    final var childInstanceKey = keyGenerator.nextKey();

    streamWriter.appendNewEvent(
        childInstanceKey, WorkflowInstanceIntent.ELEMENT_ACTIVATING, childInstanceRecord);

    return stateBehavior.createChildElementInstance(context, childInstanceKey, childInstanceRecord);
  }
}
