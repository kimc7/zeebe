/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model.transformer;

import io.zeebe.el.Expression;
import io.zeebe.engine.processor.workflow.deployment.model.BpmnStep;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableActivity;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElementContainer;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableLoopCharacteristics;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableMultiInstanceBody;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableWorkflow;
import io.zeebe.engine.processor.workflow.deployment.model.transformation.ModelElementTransformer;
import io.zeebe.engine.processor.workflow.deployment.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.Activity;
import io.zeebe.model.bpmn.instance.LoopCharacteristics;
import io.zeebe.model.bpmn.instance.MultiInstanceLoopCharacteristics;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeLoopCharacteristics;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Collections;
import java.util.Optional;
import org.agrona.DirectBuffer;

public final class MultiInstanceActivityTransformer implements ModelElementTransformer<Activity> {
  @Override
  public Class<Activity> getType() {
    return Activity.class;
  }

  @Override
  public void transform(final Activity element, final TransformContext context) {
    final ExecutableWorkflow workflow = context.getCurrentWorkflow();
    final ExecutableActivity innerActivity =
        workflow.getElementById(element.getId(), ExecutableActivity.class);

    final LoopCharacteristics loopCharacteristics = element.getLoopCharacteristics();
    if (loopCharacteristics instanceof MultiInstanceLoopCharacteristics) {

      final ExecutableLoopCharacteristics miLoopCharacteristics =
          transformLoopCharacteristics(
              context, (MultiInstanceLoopCharacteristics) loopCharacteristics);

      final ExecutableMultiInstanceBody multiInstanceBody =
          new ExecutableMultiInstanceBody(element.getId(), miLoopCharacteristics, innerActivity);

      multiInstanceBody.setElementType(BpmnElementType.MULTI_INSTANCE_BODY);

      multiInstanceBody.setFlowScope(innerActivity.getFlowScope());
      innerActivity.setFlowScope(multiInstanceBody);

      // configure lifecycle of the body
      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_ACTIVATING, BpmnStep.BPMN_ELEMENT_PROCESSOR);
      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_ACTIVATED, BpmnStep.BPMN_ELEMENT_PROCESSOR);

      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_COMPLETING, BpmnStep.BPMN_ELEMENT_PROCESSOR);
      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_COMPLETED, BpmnStep.BPMN_ELEMENT_PROCESSOR);

      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_TERMINATING, BpmnStep.MULTI_INSTANCE_TERMINATING);
      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.ELEMENT_TERMINATED, BpmnStep.ACTIVITY_ELEMENT_TERMINATED);

      multiInstanceBody.bindLifecycleState(
          WorkflowInstanceIntent.EVENT_OCCURRED, BpmnStep.MULTI_INSTANCE_EVENT_OCCURRED);

      // attach boundary events to the multi-instance body
      innerActivity.getBoundaryEvents().forEach(multiInstanceBody::attach);
      innerActivity.getEventSubprocesses().forEach(multiInstanceBody::attach);

      innerActivity.getEvents().removeAll(innerActivity.getBoundaryEvents());
      innerActivity.getEventSubprocesses().stream()
          .map(ExecutableFlowElementContainer::getStartEvents)
          .forEach(innerActivity.getEvents()::remove);

      innerActivity.getInterruptingElementIds().clear();

      // attach incoming and outgoing sequence flows to the multi-instance body
      innerActivity.getIncoming().forEach(flow -> flow.setTarget(multiInstanceBody));

      multiInstanceBody
          .getOutgoing()
          .addAll(Collections.unmodifiableList(innerActivity.getOutgoing()));
      innerActivity.getOutgoing().clear();

      // replace the inner element with the body
      workflow.addFlowElement(multiInstanceBody);
    }
  }

  private ExecutableLoopCharacteristics transformLoopCharacteristics(
      final TransformContext context,
      final MultiInstanceLoopCharacteristics elementLoopCharacteristics) {

    final boolean isSequential = elementLoopCharacteristics.isSequential();

    final ZeebeLoopCharacteristics zeebeLoopCharacteristics =
        elementLoopCharacteristics.getSingleExtensionElement(ZeebeLoopCharacteristics.class);

    final Expression inputCollection =
        context
            .getExpressionLanguage()
            .parseExpression(zeebeLoopCharacteristics.getInputCollection());

    final Optional<DirectBuffer> inputElement =
        Optional.ofNullable(zeebeLoopCharacteristics.getInputElement())
            .filter(e -> !e.isEmpty())
            .map(BufferUtil::wrapString);

    final Optional<DirectBuffer> outputCollection =
        Optional.ofNullable(zeebeLoopCharacteristics.getOutputCollection())
            .filter(e -> !e.isEmpty())
            .map(BufferUtil::wrapString);

    final Optional<Expression> outputElement =
        Optional.ofNullable(zeebeLoopCharacteristics.getOutputElement())
            .filter(e -> !e.isEmpty())
            .map(e -> context.getExpressionLanguage().parseExpression(e));

    return new ExecutableLoopCharacteristics(
        isSequential, inputCollection, inputElement, outputCollection, outputElement);
  }
}
