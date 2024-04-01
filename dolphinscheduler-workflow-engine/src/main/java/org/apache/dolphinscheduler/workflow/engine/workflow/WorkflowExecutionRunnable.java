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

import org.apache.dolphinscheduler.workflow.engine.dag.ITask;
import org.apache.dolphinscheduler.workflow.engine.dag.ITaskIdentify;
import org.apache.dolphinscheduler.workflow.engine.dag.WorkflowDAG;
import org.apache.dolphinscheduler.workflow.engine.event.TaskOperationEvent;
import org.apache.dolphinscheduler.workflow.engine.event.WorkflowFinishEvent;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

@Getter
public class WorkflowExecutionRunnable extends BaseWorkflowExecutionRunnable {

    private final WorkflowExecutionDAG workflowExecutionDAG;
    private final WorkflowDAG workflowDAG;

    private final IWorkflowExecutionRunnableDelegate workflowExecutionRunnableDelegate;

    public WorkflowExecutionRunnable(IWorkflowExecutionContext workflowExecutionContext,
                                     IWorkflowExecutionRunnableDelegate workflowExecutionRunnableDelegate) {
        super(workflowExecutionContext, WorkflowExecutionRunnableStatus.CREATED);
        this.workflowExecutionDAG = workflowExecutionContext.getWorkflowExecutionDAG();
        this.workflowDAG = workflowExecutionContext.getWorkflowDAG();
        this.workflowExecutionRunnableDelegate = workflowExecutionRunnableDelegate;
    }

    @Override
    public void start() {
        if (!workflowExecutionRunnableStatus.canStart()) {
            throw new UnsupportedOperationException(
                    "The current status: " + workflowExecutionRunnableStatus + " cannot start.");
        }

        statusTransform(WorkflowExecutionRunnableStatus.RUNNING, () -> {
            workflowExecutionRunnableDelegate.start();
            List<ITaskIdentify> startTaskIdentifies = workflowExecutionContext.getStartTaskIdentifies();
            // If the start task is empty, trigger from the beginning
            if (CollectionUtils.isEmpty(startTaskIdentifies)) {
                workflowFinish();
                return;
            }
            startTaskIdentifies.forEach(this::triggerTask);
        });
    }

    @Override
    public void triggerNextTasks(ITaskIdentify taskIdentify) {
        List<ITaskIdentify> directPostNodeIdentifies = workflowDAG.getDirectPostNodesByIdentify(taskIdentify)
                .stream()
                .map(ITask::getIdentify)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(directPostNodeIdentifies)) {
            directPostNodeIdentifies.forEach(this::triggerTask);
            return;
        }
        List<ITaskExecutionRunnableIdentify> activeTaskExecutionIdentify = getActiveTaskExecutionIdentify();
        if (CollectionUtils.isEmpty(activeTaskExecutionIdentify)) {
            workflowFinish();
            return;
        }
        // The task chain is finished, but there are still active tasks, wait for the active tasks to finish
    }

    @Override
    public void triggerTask(ITaskIdentify taskIdentify) {
        ITaskExecutionPlan taskExecutionPlan = workflowExecutionDAG.getDAGNode(taskIdentify);
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException("Cannot find the ITaskExecutionPlan for taskIdentify: " + taskIdentify);
        }
        getEventRepository().storeEventToTail(TaskOperationEvent.startEvent(taskExecutionPlan));
    }

    @Override
    public void failoverTask(ITaskExecutionRunnableIdentify taskExecutionRunnableIdentify) {
        ITaskExecutionPlan taskExecutionPlan =
                workflowExecutionDAG.getDAGNode(taskExecutionRunnableIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException("Cannot find the ITaskExecutionPlan for taskIdentify: "
                    + taskExecutionRunnableIdentify.getTaskIdentify());
        }
        getEventRepository().storeEventToTail(TaskOperationEvent.failoverEvent(taskExecutionPlan));
    }

    @Override
    public void retryTask(ITaskExecutionRunnableIdentify taskExecutionRunnableIdentify) {
        ITaskExecutionPlan taskExecutionPlan =
                workflowExecutionDAG.getDAGNode(taskExecutionRunnableIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException("Cannot find the ITaskExecutionPlan for taskIdentify: "
                    + taskExecutionRunnableIdentify.getTaskIdentify());
        }
        getEventRepository().storeEventToTail(TaskOperationEvent.retryEvent(taskExecutionPlan));
    }

    @Override
    public void pause() {
        if (workflowExecutionRunnableStatus.canPause()) {
            throw new UnsupportedOperationException(
                    "The current status: " + workflowExecutionRunnableStatus + " cannot pause.");
        }
        statusTransform(WorkflowExecutionRunnableStatus.PAUSING, () -> {
            workflowExecutionRunnableDelegate.pause();
            List<ITaskExecutionRunnableIdentify> activeTaskExecutionIdentify = getActiveTaskExecutionIdentify();
            if (CollectionUtils.isEmpty(activeTaskExecutionIdentify)) {
                workflowFinish();
                return;
            }
            activeTaskExecutionIdentify.forEach(this::pauseTask);
        });
    }

    @Override
    public void pauseTask(ITaskExecutionRunnableIdentify taskExecutionIdentify) {
        ITaskExecutionPlan taskExecutionPlan = workflowExecutionDAG.getDAGNode(taskExecutionIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException(
                    "Cannot find the ITaskExecutionPlan for taskIdentify: " + taskExecutionIdentify.getTaskIdentify());
        }
        getEventRepository().storeEventToTail(TaskOperationEvent.pauseEvent(taskExecutionPlan));
    }

    @Override
    public void kill() {
        if (workflowExecutionRunnableStatus.canKill()) {
            throw new UnsupportedOperationException(
                    "The current status: " + workflowExecutionRunnableStatus + " cannot kill.");
        }
        statusTransform(WorkflowExecutionRunnableStatus.KILLING, () -> {
            workflowExecutionRunnableDelegate.kill();
            List<ITaskExecutionRunnableIdentify> activeTaskExecutionIdentify = getActiveTaskExecutionIdentify();
            if (CollectionUtils.isEmpty(activeTaskExecutionIdentify)) {
                workflowFinish();
                return;
            }
            activeTaskExecutionIdentify.forEach(this::killTask);
        });
    }

    @Override
    public void killTask(ITaskExecutionRunnableIdentify taskExecutionIdentify) {
        ITaskExecutionPlan taskExecutionPlan = workflowExecutionDAG.getDAGNode(taskExecutionIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException(
                    "Cannot find the ITaskExecutionPlan for taskIdentify: " + taskExecutionIdentify.getTaskIdentify());
        }
        getEventRepository().storeEventToTail(TaskOperationEvent.killEvent(taskExecutionPlan));
    }

    private void workflowFinish() {
        if (workflowExecutionDAG.isFailed()) {

        }
        getEventRepository().storeEventToTail(WorkflowFinishEvent.of(workflowExecutionRunnableIdentify));
    }

    private List<ITaskExecutionRunnableIdentify> getActiveTaskExecutionIdentify() {
        return workflowExecutionDAG.getActiveTaskExecutionPlan()
                .stream()
                .map(ITaskExecutionPlan::getActiveTaskExecutionRunnable)
                .map(ITaskExecutionRunnable::getIdentify)
                .collect(Collectors.toList());
    }
}
