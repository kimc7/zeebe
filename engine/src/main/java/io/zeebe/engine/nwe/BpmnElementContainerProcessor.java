package io.zeebe.engine.nwe;

import io.zeebe.engine.processor.workflow.deployment.model.element.ExecutableFlowElement;

public interface BpmnElementContainerProcessor<T extends ExecutableFlowElement>
    extends BpmnElementProcessor<T> {

  void onChildCompleted(
      final T element,
      final BpmnElementContext flowScopeContext,
      final BpmnElementContext childContext);
}
