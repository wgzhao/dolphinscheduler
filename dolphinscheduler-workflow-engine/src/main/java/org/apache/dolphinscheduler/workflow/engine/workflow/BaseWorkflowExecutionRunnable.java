/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.workflow.engine.workflow;

import org.apache.dolphinscheduler.workflow.engine.event.IEventRepository;

public abstract class BaseWorkflowExecutionRunnable implements IWorkflowExecutionRunnable {

    protected final IWorkflowExecutionContext workflowExecutionContext;

    protected WorkflowExecutionRunnableStatus workflowExecutionRunnableStatus;

    public BaseWorkflowExecutionRunnable(IWorkflowExecutionContext workflowExecutionContext,
                                         WorkflowExecutionRunnableStatus workflowExecutionRunnableStatus) {
        this.workflowExecutionContext = workflowExecutionContext;
        this.workflowExecutionRunnableStatus = workflowExecutionRunnableStatus;
    }

    @Override
    public IWorkflowExecutionRunnableIdentify getIdentity() {
        return workflowExecutionContext.getIdentify();
    }

    @Override
    public IWorkflowExecutionContext getWorkflowExecutionContext() {
        return workflowExecutionContext;
    }

    @Override
    public IEventRepository getEventRepository() {
        return workflowExecutionContext.getEventRepository();
    }

    protected void statusTransform(WorkflowExecutionRunnableStatus targetStatus, Runnable runnable) {
        WorkflowExecutionRunnableStatus originStatus = workflowExecutionRunnableStatus;
        try {
            workflowExecutionRunnableStatus = targetStatus;
            runnable.run();
        } catch (Throwable throwable) {
            workflowExecutionRunnableStatus = originStatus;
            throw throwable;
        }
    }

}
