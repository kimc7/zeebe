/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe.container;

import io.zeebe.el.Expression;
import io.zeebe.engine.nwe.BpmnElementContext;
import io.zeebe.engine.nwe.BpmnElementProcessor;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableMultiInstanceBody;
import io.zeebe.engine.state.instance.VariablesState;
import io.zeebe.msgpack.spec.MsgPackHelper;
import io.zeebe.msgpack.spec.MsgPackWriter;
import io.zeebe.util.buffer.BufferUtil;
import java.util.List;
import java.util.Optional;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class MultiInstanceBodyProcessor
    implements BpmnElementProcessor<ExecutableMultiInstanceBody> {

  private static final DirectBuffer NIL_VALUE = new UnsafeBuffer(MsgPackHelper.NIL);
  private static final DirectBuffer LOOP_COUNTER_VARIABLE = BufferUtil.wrapString("loopCounter");

  private final MutableDirectBuffer loopCounterVariableBuffer =
      new UnsafeBuffer(new byte[Long.BYTES + 1]);
  private final DirectBuffer loopCounterVariableView = new UnsafeBuffer(0, 0);

  private final MsgPackWriter variableWriter = new MsgPackWriter();
  private final ExpandableArrayBuffer variableBuffer = new ExpandableArrayBuffer();

  private final ExpressionProcessor expressionBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final CatchEventBehavior eventSubscriptionBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final VariablesState variablesState;

  public MultiInstanceBodyProcessor(final BpmnBehaviors bpmnBehaviors) {
    stateTransitionBehavior = bpmnBehaviors.stateTransitionBehavior();
    eventSubscriptionBehavior = bpmnBehaviors.eventSubscriptionBehavior();
    stateBehavior = bpmnBehaviors.stateBehavior();
    variablesState = stateBehavior.getVariablesState();
    expressionBehavior = bpmnBehaviors.expressionBehavior();
  }

  @Override
  public Class<ExecutableMultiInstanceBody> getType() {
    return ExecutableMultiInstanceBody.class;
  }

  @Override
  public void onActivating(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {

    // verify that the input collection variable is present and valid
    final Optional<List<DirectBuffer>> results = readInputCollectionVariable(element, context);
    if (results.isEmpty()) {
      return;
    }

    eventSubscriptionBehavior.subscribeToEvents(context.toStepContext(), element);

    stateTransitionBehavior.transitionToActivated(context);
  }

  @Override
  public void onActivated(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {

    final var loopCharacteristics = element.getLoopCharacteristics();
    final Optional<List<DirectBuffer>> inputCollection =
        readInputCollectionVariable(element, context);
    if (inputCollection.isEmpty()) {
      return;
    }

    final var array = inputCollection.get();
    loopCharacteristics
        .getOutputCollection()
        .ifPresent(variableName -> initializeOutputCollection(context, variableName, array.size()));

    if (array.isEmpty()) {
      // complete the multi-instance body immediately
      stateTransitionBehavior.transitionToCompleting(context);
      return;
    }

    if (loopCharacteristics.isSequential()) {
      final var firstItem = array.get(0);
      createInnerInstance(element, context, firstItem);

    } else {
      array.forEach(item -> createInnerInstance(element, context, item));
    }
  }

  @Override
  public void onCompleting(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {

    eventSubscriptionBehavior.unsubscribeFromEvents(
        context.getElementInstanceKey(), context.toStepContext());

    element
        .getLoopCharacteristics()
        .getOutputCollection()
        .ifPresent(variableName -> stateBehavior.propagateVariable(context, variableName));

    stateTransitionBehavior.transitionToCompleted(context);
  }

  @Override
  public void onCompleted(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {}

  @Override
  public void onTerminating(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {}

  @Override
  public void onTerminated(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {}

  @Override
  public void onEventOccurred(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {}

  private Optional<List<DirectBuffer>> readInputCollectionVariable(
      final ExecutableMultiInstanceBody element, final BpmnElementContext context) {
    final Expression inputCollection = element.getLoopCharacteristics().getInputCollection();
    return expressionBehavior.evaluateArrayExpression(inputCollection, context.toStepContext());
  }

  private void createInnerInstance(
      final ExecutableMultiInstanceBody multiInstanceBody,
      final BpmnElementContext context,
      final DirectBuffer item) {

    final var innerInstance =
        stateTransitionBehavior.activateChildInstance(
            context, multiInstanceBody.getInnerActivity());

    // update loop counters
    final var bodyInstance = stateBehavior.getElementInstance(context);
    bodyInstance.spawnToken();
    bodyInstance.incrementMultiInstanceLoopCounter();
    stateBehavior.updateElementInstance(bodyInstance);

    innerInstance.setMultiInstanceLoopCounter(bodyInstance.getMultiInstanceLoopCounter());
    stateBehavior.updateElementInstance(innerInstance);

    // set instance variables
    final var loopCharacteristics = multiInstanceBody.getLoopCharacteristics();

    loopCharacteristics
        .getInputElement()
        .ifPresent(
            variableName ->
                variablesState.setVariableLocal(
                    innerInstance.getKey(), context.getWorkflowKey(), variableName, item));

    // Output element expressions that are just a variable or nested property of a variable need to
    // be initialised with a nil-value. This makes sure that they are not written at a non-local
    // scope.
    loopCharacteristics
        .getOutputElement()
        .flatMap(Expression::getVariableName)
        .map(BufferUtil::wrapString)
        .ifPresent(
            variableName ->
                variablesState.setVariableLocal(
                    innerInstance.getKey(), context.getWorkflowKey(), variableName, NIL_VALUE));

    variablesState.setVariableLocal(
        innerInstance.getKey(),
        context.getWorkflowKey(),
        LOOP_COUNTER_VARIABLE,
        wrapLoopCounter(innerInstance.getMultiInstanceLoopCounter()));
  }

  private DirectBuffer wrapLoopCounter(final int loopCounter) {
    variableWriter.wrap(loopCounterVariableBuffer, 0);

    variableWriter.writeInteger(loopCounter);
    final var length = variableWriter.getOffset();

    loopCounterVariableView.wrap(loopCounterVariableBuffer, 0, length);
    return loopCounterVariableView;
  }

  private void initializeOutputCollection(
      final BpmnElementContext context, final DirectBuffer variableName, final int size) {

    variableWriter.wrap(variableBuffer, 0);

    // initialize the array with nil
    variableWriter.writeArrayHeader(size);
    for (var i = 0; i < size; i++) {
      variableWriter.writeNil();
    }

    final var length = variableWriter.getOffset();

    stateBehavior.setLocalVariable(context, variableName, variableBuffer, 0, length);
  }
}
