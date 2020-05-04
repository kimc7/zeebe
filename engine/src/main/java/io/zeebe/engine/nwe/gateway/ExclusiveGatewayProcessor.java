/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.gateway;

import io.zeebe.el.Expression;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.nwe.behavior.DeferredRecordsBehavior;
import io.zeebe.engine.processor.Failure;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableExclusiveGateway;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableSequenceFlow;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.util.Either;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Optional;

public class ExclusiveGatewayProcessor implements BpmnElementProcessor<ExecutableExclusiveGateway> {

  private static final String CONDITION_EVALUATION_ERROR =
      "Expected to evaluate condition expression '%s' for sequence flow '%s', but the evaluation failed.";
  private static final String NO_OUTGOING_FLOW_CHOSEN_ERROR =
      "Expected at least one condition to evaluate to true, or to have a default flow";
  private final WorkflowInstanceRecord record = new WorkflowInstanceRecord();

  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final DeferredRecordsBehavior deferredRecordsBehavior;
  private final ExpressionProcessor expressionBehavior;

  public ExclusiveGatewayProcessor(final BpmnBehaviors behaviors) {
    expressionBehavior = behaviors.expressionBehavior();
    incidentBehavior = behaviors.incidentBehavior();
    stateBehavior = behaviors.stateBehavior();
    deferredRecordsBehavior = behaviors.deferredRecordsBehavior();
    stateTransitionBehavior = behaviors.stateTransitionBehavior();
  }

  @Override
  public Class<ExecutableExclusiveGateway> getType() {
    return ExecutableExclusiveGateway.class;
  }

  @Override
  public void onActivating(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    if (element.getOutgoing().isEmpty()) {
      // there are no flows to take: the gateway is an implicit end for the flow scope
      stateTransitionBehavior.transitionToActivated(context);
      return;
    }

    // find outgoing sequence flow with fulfilled condition or the default or none
    findSequenceFlowToTake(element, context)
        .ifRight(
            sequenceFlow -> {
              stateTransitionBehavior.transitionToActivated(context);
              // defer sequence flow taken, as it will only be taken when the gateway is completed
              record.wrap(context.getRecordValue());
              record.setElementId(sequenceFlow.getId());
              record.setBpmnElementType(BpmnElementType.SEQUENCE_FLOW);
              deferredRecordsBehavior.deferNewRecord(
                  context.getElementInstanceKey(),
                  record,
                  WorkflowInstanceIntent.SEQUENCE_FLOW_TAKEN);
            });
  }

  @Override
  public void onActivated(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    stateTransitionBehavior.transitionToCompleting(context);
  }

  @Override
  public void onCompleting(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    stateTransitionBehavior.transitionToCompleted(context);
  }

  @Override
  public void onCompleted(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    deferredRecordsBehavior.publishDeferredRecords(context);

    stateTransitionBehavior.onCompleted(element, context);
    stateBehavior.consumeToken(context);
  }

  @Override
  public void onTerminating(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    stateTransitionBehavior.transitionToTerminated(context);
  }

  @Override
  public void onTerminated(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    // for all activities:
    // publish deferred events (i.e. an occurred boundary event)
    deferredRecordsBehavior.publishDeferredRecords(context);

    // resolve incidents
    incidentBehavior.resolveIncidents(context);

    // terminate scope if scope is terminated and last active token
    // publish deferred event if an interrupting event sub-process was triggered
    stateBehavior.terminateFlowScope(context); // interruption is part of this (still)

    // consume token
    stateBehavior.consumeToken(context);
  }

  @Override
  public void onEventOccurred(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    throw new UnsupportedOperationException(
        "expected to handle occurred event on exclusive gateway, but events should not occur on exclusive gateway");
  }

  private Either<Failure, ExecutableSequenceFlow> findSequenceFlowToTake(
      final ExecutableExclusiveGateway element, final BpmnElementContext context) {
    if (element.getOutgoing().size() == 1 && element.getOutgoing().get(0).getCondition() == null) {
      // only one flow without a condition, can just be taken
      return Either.right(element.getOutgoing().get(0));
    }
    for (final ExecutableSequenceFlow sequenceFlow : element.getOutgoingWithCondition()) {
      final Expression condition = sequenceFlow.getCondition();
      final Optional<Boolean> isFulfilled =
          expressionBehavior.evaluateBooleanExpression(condition, context.toStepContext());
      if (isFulfilled.isEmpty()) {
        // the condition evaluation failed and an incident is raised
        // todo(@korthout): move the incident raising into this method (or even higher)
        final String sequenceFlowId = BufferUtil.bufferAsString(sequenceFlow.getId());
        final String message =
            String.format(CONDITION_EVALUATION_ERROR, condition.getExpression(), sequenceFlowId);
        return Either.left(new Failure(message));

      } else if (isFulfilled.get()) {
        // the condition is fulfilled
        return Either.right(sequenceFlow);
      }
    }
    // no condition is fulfilled - try to take the default flow
    if (element.getDefaultFlow() != null) {
      return Either.right(element.getDefaultFlow());
    }
    incidentBehavior.createIncident(
        ErrorType.CONDITION_ERROR,
        NO_OUTGOING_FLOW_CHOSEN_ERROR,
        context,
        context.getElementInstanceKey());
    return Either.left(new Failure(NO_OUTGOING_FLOW_CHOSEN_ERROR));
  }
}
