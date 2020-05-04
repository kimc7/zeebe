/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.nwe;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.nwe.behavior.BpmnBehaviors;
import io.zeebe.engine.nwe.behavior.BpmnBehaviorsImpl;
import io.zeebe.engine.nwe.behavior.BpmnIncidentBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateBehavior;
import io.zeebe.engine.nwe.behavior.BpmnStateTransitionBehavior;
import io.zeebe.engine.nwe.behavior.DeferredRecordsBehavior;
import io.zeebe.engine.nwe.behavior.TypesStreamWriterProxy;
import io.zeebe.engine.nwe.container.MultiInstanceBodyProcessor;
import io.zeebe.engine.nwe.container.ProcessProcessor;
import io.zeebe.engine.nwe.container.SubProcessProcessor;
import io.zeebe.engine.nwe.gateway.ExclusiveGatewayProcessor;
import io.zeebe.engine.nwe.task.ServiceTaskProcessor;
import io.zeebe.engine.processor.SideEffectProducer;
import io.zeebe.engine.processor.TypedRecord;
import io.zeebe.engine.processor.TypedRecordProcessor;
import io.zeebe.engine.processor.TypedResponseWriter;
import io.zeebe.engine.processor.TypedStreamWriter;
import io.zeebe.engine.processor.workflow.BpmnStepContext;
import io.zeebe.engine.processor.workflow.CatchEventBehavior;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableMultiInstanceBody;
import io.zeebe.engine.processor.workflow.handlers.IOMappingHelper;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.deployment.WorkflowState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;

public final class BpmnStreamProcessor implements TypedRecordProcessor<WorkflowInstanceRecord> {

  private static final Logger LOGGER = Loggers.WORKFLOW_PROCESSOR_LOGGER;

  private final TypesStreamWriterProxy streamWriterProxy = new TypesStreamWriterProxy();

  private final BpmnElementContextImpl context;
  private final WorkflowState workflowState;

  private final Map<BpmnElementType, BpmnElementProcessor<?>> processors;

  private final Consumer<BpmnStepContext<?>> fallback;

  public BpmnStreamProcessor(
      final ExpressionProcessor expressionProcessor,
      final IOMappingHelper ioMappingHelper,
      final CatchEventBehavior catchEventBehavior,
      final ZeebeState zeebeState,
      final Consumer<BpmnStepContext<?>> fallback) {

    workflowState = zeebeState.getWorkflowState();
    this.fallback = fallback;

    final var stateBehavior = new BpmnStateBehavior(zeebeState, streamWriterProxy);
    final BpmnBehaviors bpmnBehaviors =
        new BpmnBehaviorsImpl(
            expressionProcessor,
            ioMappingHelper,
            catchEventBehavior,
            new BpmnIncidentBehavior(zeebeState, streamWriterProxy),
            stateBehavior,
            new BpmnStateTransitionBehavior(
                streamWriterProxy,
                zeebeState.getKeyGenerator(),
                stateBehavior,
                this::getContainerProcessor),
            streamWriterProxy,
            new DeferredRecordsBehavior(zeebeState, streamWriterProxy));

    processors =
        Map.of(
            BpmnElementType.SERVICE_TASK,
            new ServiceTaskProcessor(bpmnBehaviors),
            BpmnElementType.EXCLUSIVE_GATEWAY,
            new ExclusiveGatewayProcessor(bpmnBehaviors),
            BpmnElementType.MULTI_INSTANCE_BODY,
            new MultiInstanceBodyProcessor(bpmnBehaviors),
            BpmnElementType.SUB_PROCESS,
            new SubProcessProcessor(bpmnBehaviors),
            BpmnElementType.PROCESS,
            new ProcessProcessor(bpmnBehaviors));

    context = new BpmnElementContextImpl(zeebeState);
  }

  private <T extends ExecutableFlowElement> BpmnElementProcessor<T> getProcessor(
      final BpmnElementType bpmnElementType) {
    if (bpmnElementType == BpmnElementType.SUB_PROCESS
        || bpmnElementType == BpmnElementType.PROCESS) {
      return null;
    }

    final var processor = (BpmnElementProcessor<T>) processors.get(bpmnElementType);
    if (processor == null) {
      LOGGER.info("[NEW] No processor found for BPMN element type '{}'", bpmnElementType);
      //      throw new UnsupportedOperationException(
      //          String.format("no processor found for BPMN element type '%s'", bpmnElementType));
    }
    return processor;
  }

  private <T extends ExecutableFlowElement> BpmnElementContainerProcessor<T> getContainerProcessor(
      final BpmnElementType bpmnElementType) {
    switch (bpmnElementType) {
      case PROCESS:
      case SUB_PROCESS:
      case MULTI_INSTANCE_BODY:
        return (BpmnElementContainerProcessor<T>) processors.get(bpmnElementType);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "no container processor found for BPMN element type '%s'", bpmnElementType));
    }
  }

  @Override
  public void processRecord(
      final TypedRecord<WorkflowInstanceRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final var intent = (WorkflowInstanceIntent) record.getIntent();
    final var recordValue = record.getValue();
    final var bpmnElementType = recordValue.getBpmnElementType();
    final var processor = getProcessor(bpmnElementType);

    final ExecutableFlowElement element = getElement(recordValue, processor);

    LOGGER.info(
        "[NEW] process workflow instance event [BPMN element type: {}, intent: {}]",
        bpmnElementType,
        intent);

    // initialize the stuff
    streamWriterProxy.wrap(streamWriter);
    context.init(record, intent, element, streamWriterProxy, sideEffect);

    if (processor == null) {
      // TODO (saig0): remove multi-instance fallback
      fallback.accept(context.toStepContext());
      return;
    }

    // process the event
    processEvent(intent, processor, element);
  }

  private ExecutableFlowElement getElement(
      final WorkflowInstanceRecord recordValue,
      final BpmnElementProcessor<ExecutableFlowElement> processor) {

    // TODO (saig0): handle multi-instance body
    final var element =
        workflowState
            .getWorkflowByKey(recordValue.getWorkflowKey())
            .getWorkflow()
            .getElementById(recordValue.getElementIdBuffer());

    if (element instanceof ExecutableMultiInstanceBody
        && recordValue.getBpmnElementType() != BpmnElementType.MULTI_INSTANCE_BODY) {
      final var multiInstanceBody = (ExecutableMultiInstanceBody) element;
      return multiInstanceBody.getInnerActivity();
    }

    return workflowState.getFlowElement(
        recordValue.getWorkflowKey(), recordValue.getElementIdBuffer(), processor.getType());
  }

  private void processEvent(
      final WorkflowInstanceIntent intent,
      final BpmnElementProcessor<ExecutableFlowElement> processor,
      final ExecutableFlowElement element) {

    switch (intent) {
      case ELEMENT_ACTIVATING:
        processor.onActivating(element, context);
        break;
      case ELEMENT_ACTIVATED:
        processor.onActivated(element, context);
        break;
      case EVENT_OCCURRED:
        processor.onEventOccurred(element, context);
        break;
      case ELEMENT_COMPLETING:
        processor.onCompleting(element, context);
        break;
      case ELEMENT_COMPLETED:
        processor.onCompleted(element, context);
        break;
      case ELEMENT_TERMINATING:
        processor.onTerminating(element, context);
        break;
      case ELEMENT_TERMINATED:
        processor.onTerminated(element, context);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "processor '%s' can not handle intent '%s'", processor.getClass(), intent));
    }
  }
}
