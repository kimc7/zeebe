/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.task;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.nwe.behavior.DeferredRecordsBehavior;
import io.zeebe.engine.processor.Failure;
import io.zeebe.engine.processor.TypedCommandWriter;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.ExpressionProcessor.EvaluationException;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableBoundaryEvent;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableServiceTask;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;
import io.zeebe.engine.processor.workflow.message.MessageCorrelationKeyException;
import io.zeebe.engine.state.instance.EventTrigger;
import io.zeebe.engine.state.instance.JobState.State;
import io.zeebe.msgpack.value.DocumentValue;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.util.Either;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Optional;

public final class ServiceTaskProcessor implements BpmnElementProcessor<ExecutableServiceTask> {

  private final JobRecord jobCommand = new JobRecord().setVariables(DocumentValue.EMPTY_DOCUMENT);
  private final WorkflowInstanceRecord eventRecord = new WorkflowInstanceRecord();

  private final IOMappingHelper variableMappingBehavior;
  private final CatchEventBehavior eventSubscriptionBehavior;
  private final ExpressionProcessor expressionBehavior;
  private final TypedCommandWriter commandWriter;
  private final DeferredRecordsBehavior deferredRecordsBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;

  public ServiceTaskProcessor(final BpmnBehaviors behaviors) {
    variableMappingBehavior = behaviors.variableMappingBehavior();
    eventSubscriptionBehavior = behaviors.eventSubscriptionBehavior();
    expressionBehavior = behaviors.expressionBehavior();
    commandWriter = behaviors.commandWriter();
    deferredRecordsBehavior = behaviors.deferredRecordsBehavior();
    incidentBehavior = behaviors.incidentBehavior();
    stateBehavior = behaviors.stateBehavior();
    stateTransitionBehavior = behaviors.stateTransitionBehavior();
  }

  @Override
  public Class<ExecutableServiceTask> getType() {
    return ExecutableServiceTask.class;
  }

  @Override
  public void onActivating(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // input mappings
    // subscribe to events

    // TODO (saig0): migrate to Either types
    final var success = variableMappingBehavior.applyInputMappings(context.toStepContext());
    if (!success) {
      return;
    }

    try {
      eventSubscriptionBehavior.subscribeToEvents(context.toStepContext(), element);
    } catch (final MessageCorrelationKeyException e) {
      incidentBehavior.createIncident(
          ErrorType.EXTRACT_VALUE_ERROR,
          e.getMessage(),
          context,
          e.getContext().getVariablesScopeKey());
      return;
    } catch (final EvaluationException e) {
      incidentBehavior.createIncident(
          ErrorType.EXTRACT_VALUE_ERROR, e.getMessage(), context, context.getElementInstanceKey());
      return;
    }

    stateTransitionBehavior.transitionToActivated(context);
  }

  @Override
  public void onActivated(final ExecutableServiceTask element, final BpmnElementContext context) {
    // only for service task:
    // evaluate job type expression
    // evaluate job retries expression
    // create job

    // --> may be better done on activating

    final Either<Failure, String> optJobType =
        expressionBehavior.evaluateStringExpression(
            element.getType(), context.getElementInstanceKey());

    // TODO (saig0): I want either.flapMap and consuming methods =)
    if (optJobType.isLeft()) {
      final var failure = optJobType.getLeft();
      incidentBehavior.createIncident(
          ErrorType.EXTRACT_VALUE_ERROR,
          failure.getMessage(),
          context,
          context.getElementInstanceKey());
      return;
    }

    final Optional<Long> optRetries =
        expressionBehavior.evaluateLongExpression(element.getRetries(), context.toStepContext());

    if (optJobType.isRight() && optRetries.isPresent()) {
      createNewJob(context, element, optJobType.get(), optRetries.get().intValue());
    }
  }

  @Override
  public void onCompleting(final ExecutableServiceTask element, final BpmnElementContext context) {

    // for all activities:
    // output mappings
    // unsubscribe from events

    // TODO (saig0): extract guard check and perform also on other transitions
    final var flowScopeInstance = stateBehavior.getFlowScopeInstance(context);
    if (!flowScopeInstance.isActive()) {
      return;
    }

    final var success = variableMappingBehavior.applyOutputMappings(context.toStepContext());
    if (!success) {
      return;
    }

    eventSubscriptionBehavior.unsubscribeFromEvents(
        context.getElementInstanceKey(), context.toStepContext());

    stateTransitionBehavior.transitionToCompleted(context);

    // TODO (saig0): shutdown event scope? (sse AbstractHandler#transitionTo)
  }

  @Override
  public void onCompleted(final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // take outgoing sequence flows
    // complete scope if last active token
    // consume token
    // remove from event scope instance state
    // remove from element instance state

    final var outgoingSequenceFlows = element.getOutgoing();
    if (outgoingSequenceFlows.isEmpty()) {

      if (stateBehavior.isLastActiveExecutionPathInScope(context)) {
        stateBehavior.completeFlowScope(context);

        // TODO (saig0): update state because of the step guards
        stateBehavior.updateFlowScopeInstance(
            context,
            elementInstance -> elementInstance.setState(WorkflowInstanceIntent.ELEMENT_COMPLETING));
      }

    } else {
      outgoingSequenceFlows.forEach(
          sequenceFlow -> {
            stateTransitionBehavior.takeSequenceFlow(context, sequenceFlow);
            stateBehavior.spawnToken(context);
          });
    }

    stateBehavior.consumeToken(context);
    stateBehavior.removeInstance(context);
  }

  @Override
  public void onTerminating(final ExecutableServiceTask element, final BpmnElementContext context) {
    // only for service task:
    // cancel job
    // resolve job incident

    // for all activities:
    // unsubscribe from events

    final var elementInstance = stateBehavior.getElementInstance(context);
    final long jobKey = elementInstance.getJobKey();
    if (jobKey > 0) {
      cancelJob(jobKey);
      incidentBehavior.resolveJobIncident(jobKey);
    }

    eventSubscriptionBehavior.unsubscribeFromEvents(
        context.getElementInstanceKey(), context.toStepContext());

    stateTransitionBehavior.transitionToTerminated(context);

    // TODO (saig0): shutdown event scope? (sse AbstractHandler#transitionTo)
  }

  @Override
  public void onTerminated(final ExecutableServiceTask element, final BpmnElementContext context) {
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
      final ExecutableServiceTask element, final BpmnElementContext context) {
    // for all activities:
    // (when boundary event is triggered)

    final EventTrigger event =
        context
            .toStepContext()
            .getStateDb()
            .getEventScopeInstanceState()
            .peekEventTrigger(context.getElementInstanceKey());

    final ExecutableBoundaryEvent boundaryEvent = getBoundaryEvent(element, event);
    if (boundaryEvent == null) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.error(
          "No boundary event found with ID {} for process {}",
          BufferUtil.bufferAsString(event.getElementId()),
          context.getWorkflowKey());
      return;
    }
    final WorkflowInstanceRecord eventRecord =
        fillEventRecord(context, event, boundaryEvent.getElementType());
    final long boundaryInstanceKey;
    if (boundaryEvent.interrupting()) {
      // if interrupting then terminate element and defer occurred event
      stateTransitionBehavior.transitionToTerminating(context);
      boundaryInstanceKey =
          deferredRecordsBehavior.deferNewRecord(
              context.getElementInstanceKey(),
              eventRecord,
              WorkflowInstanceIntent.ELEMENT_ACTIVATING);
    } else {
      // if non-interrupting then activate boundary event and spawn token
      boundaryInstanceKey =
          stateTransitionBehavior.activateBoundaryInstance(context, eventRecord, event);
    }

    // move variables over to new flow
    stateBehavior
        .getVariablesState()
        .setTemporaryVariables(boundaryInstanceKey, event.getVariables());

    // clean-up the event trigger
    stateBehavior.deleteTrigger(context.getElementInstanceKey(), event.getEventKey());
  }

  private ExecutableBoundaryEvent getBoundaryEvent(
      final ExecutableServiceTask element, final EventTrigger event) {
    for (final ExecutableBoundaryEvent boundaryEvent : element.getBoundaryEvents()) {
      if (event.getElementId().equals(boundaryEvent.getId())) {
        return boundaryEvent;
      }
    }
    return null;
  }

  protected WorkflowInstanceRecord fillEventRecord(
      final BpmnElementContext context,
      final EventTrigger event,
      final BpmnElementType elementType) {
    eventRecord.reset();
    eventRecord.wrap(context.getRecordValue());
    eventRecord.setElementId(event.getElementId());
    eventRecord.setBpmnElementType(elementType);
    return eventRecord;
  }

  private void createNewJob(
      final BpmnElementContext context,
      final ExecutableServiceTask serviceTask,
      final String jobType,
      final int retries) {

    jobCommand
        .setType(jobType)
        .setRetries(retries)
        .setCustomHeaders(serviceTask.getEncodedHeaders())
        .setBpmnProcessId(context.getBpmnProcessId())
        .setWorkflowDefinitionVersion(context.getWorkflowVersion())
        .setWorkflowKey(context.getWorkflowKey())
        .setWorkflowInstanceKey(context.getWorkflowInstanceKey())
        .setElementId(serviceTask.getId())
        .setElementInstanceKey(context.getElementInstanceKey());

    commandWriter.appendNewCommand(JobIntent.CREATE, jobCommand);
  }

  private void cancelJob(final long jobKey) {
    final State state = stateBehavior.getJobState().getState(jobKey);

    if (state == State.NOT_FOUND) {
      Loggers.WORKFLOW_PROCESSOR_LOGGER.warn(
          "Expected to find job with key {}, but no job found", jobKey);

    } else if (state == State.ACTIVATABLE || state == State.ACTIVATED || state == State.FAILED) {
      final JobRecord job = stateBehavior.getJobState().getJob(jobKey);
      commandWriter.appendFollowUpCommand(jobKey, JobIntent.CANCEL, job);
    }
  }
}
