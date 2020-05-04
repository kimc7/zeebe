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
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.instance.ElementInstanceState;
import io.zeebe.engine.state.instance.IndexedRecord;
import io.zeebe.engine.state.instance.WorkflowEngineState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import java.util.List;

public class DeferredRecordsBehavior {

  private final TypedStreamWriter streamWriter;
  private final WorkflowEngineState workflowEngineState;
  private final ElementInstanceState elementInstanceState;
  private final KeyGenerator keyGenerator;

  public DeferredRecordsBehavior(
      final ZeebeState zeebeState, final TypedStreamWriter streamWriter) {
    this.workflowEngineState = new WorkflowEngineState(1, zeebeState.getWorkflowState());
    this.elementInstanceState = zeebeState.getWorkflowState().getElementInstanceState();
    this.streamWriter = streamWriter;
    this.keyGenerator = zeebeState.getKeyGenerator();
  }

  public long deferNewRecord(
      final long scopeKey, final WorkflowInstanceRecord value, final WorkflowInstanceIntent state) {
    final long key = keyGenerator.nextKey();
    workflowEngineState.deferRecord(key, scopeKey, value, state);
    return key;
  }

  public void publishDeferredRecords(final BpmnElementContext context) {
    final List<IndexedRecord> deferredRecords =
        workflowEngineState.getDeferredRecords(context.getElementInstanceKey());
    final ElementInstance flowScopeInstance =
        elementInstanceState.getInstance(context.getRecordValue().getFlowScopeKey());
    for (final IndexedRecord record : deferredRecords) {
      record.getValue().setFlowScopeKey(flowScopeInstance.getKey());
      if (record.getState().equals(WorkflowInstanceIntent.ELEMENT_ACTIVATING)) {
        elementInstanceState.newInstance(
            flowScopeInstance, record.getKey(), record.getValue(), record.getState());
      }
      streamWriter.appendFollowUpEvent(record.getKey(), record.getState(), record.getValue());
      elementInstanceState.spawnToken(flowScopeInstance.getKey());
    }
  }
}
