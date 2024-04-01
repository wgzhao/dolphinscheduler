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

package org.apache.dolphinscheduler.workflow.engine.engine;

import org.apache.dolphinscheduler.workflow.engine.dag.ITask;
import org.apache.dolphinscheduler.workflow.engine.dag.ITaskIdentify;
import org.apache.dolphinscheduler.workflow.engine.dag.WorkflowDAG;
import org.apache.dolphinscheduler.workflow.engine.event.IEventRepository;
import org.apache.dolphinscheduler.workflow.engine.event.TaskOperationEvent;
import org.apache.dolphinscheduler.workflow.engine.event.WorkflowFinishEvent;
import org.apache.dolphinscheduler.workflow.engine.workflow.ITaskExecutionPlan;
import org.apache.dolphinscheduler.workflow.engine.workflow.ITaskExecutionRunnable;
import org.apache.dolphinscheduler.workflow.engine.workflow.ITaskExecutionRunnableIdentify;
import org.apache.dolphinscheduler.workflow.engine.workflow.IWorkflowExecutionContext;
import org.apache.dolphinscheduler.workflow.engine.workflow.IWorkflowExecutionRunnableIdentify;
import org.apache.dolphinscheduler.workflow.engine.workflow.WorkflowExecutionDAG;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Deprecated
@Slf4j
public class DAGEngine implements IDAGEngine {

    private final IWorkflowExecutionContext workflowExecutionContext;

    private final IEventRepository eventRepository;
    private final WorkflowExecutionDAG workflowExecutionDAG;
    private final WorkflowDAG workflowDAG;

    private final IWorkflowExecutionRunnableIdentify workflowExecutionRunnableIdentify;

    public DAGEngine(IWorkflowExecutionContext workflowExecutionContext) {
        this.workflowExecutionContext = workflowExecutionContext;

        this.workflowExecutionRunnableIdentify = workflowExecutionContext.getIdentify();
        this.eventRepository = workflowExecutionContext.getEventRepository();
        this.workflowDAG = workflowExecutionContext.getWorkflowDAG();
        this.workflowExecutionDAG = workflowExecutionContext.getWorkflowExecutionDAG();
    }

    @Override
    public void start() {
        List<ITaskIdentify> startTaskIdentifies = workflowExecutionContext.getStartTaskIdentifies();
        // If the start task is empty, trigger from the beginning
        if (CollectionUtils.isEmpty(startTaskIdentifies)) {
            startTaskIdentifies = workflowDAG.getDirectPostNodesByIdentify(null)
                    .stream()
                    .map(ITask::getIdentify)
                    .collect(Collectors.toList());
        }

        if (CollectionUtils.isEmpty(startTaskIdentifies)) {
            workflowFinish();
            return;
        }
        startTaskIdentifies.forEach(this::triggerTask);
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
        eventRepository.storeEventToTail(TaskOperationEvent.startEvent(taskExecutionPlan));
    }

    @Override
    public void failoverTask(ITaskExecutionRunnableIdentify taskExecutionRunnableIdentify) {
        ITaskExecutionPlan taskExecutionPlan =
                workflowExecutionDAG.getDAGNode(taskExecutionRunnableIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException("Cannot find the ITaskExecutionPlan for taskIdentify: "
                    + taskExecutionRunnableIdentify.getTaskIdentify());
        }
        eventRepository.storeEventToTail(TaskOperationEvent.failoverEvent(taskExecutionPlan));
    }

    @Override
    public void retryTask(ITaskExecutionRunnableIdentify taskExecutionRunnableIdentify) {
        ITaskExecutionPlan taskExecutionPlan =
                workflowExecutionDAG.getDAGNode(taskExecutionRunnableIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException("Cannot find the ITaskExecutionPlan for taskIdentify: "
                    + taskExecutionRunnableIdentify.getTaskIdentify());
        }
        eventRepository.storeEventToTail(TaskOperationEvent.retryEvent(taskExecutionPlan));
    }

    @Override
    public void pause() {
        List<ITaskExecutionRunnableIdentify> activeTaskExecutionIdentify = getActiveTaskExecutionIdentify();
        if (CollectionUtils.isEmpty(activeTaskExecutionIdentify)) {
            workflowFinish();
            return;
        }
        activeTaskExecutionIdentify.forEach(this::pauseTask);
    }

    @Override
    public void pauseTask(ITaskExecutionRunnableIdentify taskExecutionIdentify) {
        ITaskExecutionPlan taskExecutionPlan = workflowExecutionDAG.getDAGNode(taskExecutionIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException(
                    "Cannot find the ITaskExecutionPlan for taskIdentify: " + taskExecutionIdentify.getTaskIdentify());
        }
        eventRepository.storeEventToTail(TaskOperationEvent.pauseEvent(taskExecutionPlan));
    }

    @Override
    public void kill() {
        List<ITaskExecutionRunnableIdentify> activeTaskExecutionIdentify = getActiveTaskExecutionIdentify();
        if (CollectionUtils.isEmpty(activeTaskExecutionIdentify)) {
            workflowFinish();
            return;
        }
        activeTaskExecutionIdentify.forEach(this::killTask);
    }

    @Override
    public void killTask(ITaskExecutionRunnableIdentify taskExecutionIdentify) {
        ITaskExecutionPlan taskExecutionPlan = workflowExecutionDAG.getDAGNode(taskExecutionIdentify.getTaskIdentify());
        if (taskExecutionPlan == null) {
            throw new IllegalArgumentException(
                    "Cannot find the ITaskExecutionPlan for taskIdentify: " + taskExecutionIdentify.getTaskIdentify());
        }
        eventRepository.storeEventToTail(TaskOperationEvent.killEvent(taskExecutionPlan));
    }

    private void workflowFinish() {
        if (workflowExecutionDAG.isFailed()) {

        }
        eventRepository.storeEventToTail(WorkflowFinishEvent.of(workflowExecutionRunnableIdentify));
    }

    private List<ITaskExecutionRunnableIdentify> getActiveTaskExecutionIdentify() {
        return workflowExecutionDAG.getActiveTaskExecutionPlan()
                .stream()
                .map(ITaskExecutionPlan::getActiveTaskExecutionRunnable)
                .map(ITaskExecutionRunnable::getIdentify)
                .collect(Collectors.toList());
    }

}
