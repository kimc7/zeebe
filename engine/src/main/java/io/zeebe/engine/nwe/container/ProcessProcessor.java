/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.container;

import io.zeebe.engine.nwe.BpmnElementContainerProcessor;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElementContainer;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;

public final class ProcessProcessor
    implements BpmnElementContainerProcessor<ExecutableFlowElementContainer> {

  private final BpmnStateBehavior stateBehavior;

  public ProcessProcessor(final BpmnBehaviors bpmnBehaviors) {
    stateBehavior = bpmnBehaviors.stateBehavior();
  }

  @Override
  public void onChildCompleted(
      final ExecutableFlowElementContainer element,
      final BpmnElementContext flowScopeContext,
      final BpmnElementContext childContext) {

    if (stateBehavior.isLastActiveExecutionPathInScope(childContext)) {
      stateBehavior.completeFlowScope(childContext);

      stateBehavior.updateFlowScopeInstance(
          childContext,
          elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_COMPLETING));
    }
  }

  @Override
  public Class<ExecutableFlowElementContainer> getType() {
    return ExecutableFlowElementContainer.class;
  }

  @Override
  public void onActivating(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onActivated(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onCompleting(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onCompleted(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onTerminating(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onTerminated(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}

  @Override
  public void onEventOccurred(
      final ExecutableFlowElementContainer element, final BpmnElementContext context) {}
}
