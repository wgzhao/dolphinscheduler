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

import java.util.List;
import java.util.Map;

public class TaskExecutionPlan implements ITaskExecutionPlan {

    private ITaskIdentify taskIdentify;

    private ITask task;

    private IWorkflowExecutionContext workflowExecutionContext;

    private ITaskExecutionRunnableFactory taskExecutionRunnableFactory;

    private Map<ITaskExecutionRunnableIdentify, ITaskExecutionRunnable> taskExecutionRunnableMap;

    private ITaskExecutionRunnable activeTaskExecutionRunnable;

    @Override
    public void start() {
        ITaskExecutionRunnable taskExecutionRunnable =
                taskExecutionRunnableFactory.createTaskExecutionRunnable(taskIdentify, workflowExecutionContext);
        taskExecutionRunnableMap.put(taskExecutionRunnable.getIdentify(), taskExecutionRunnable);
        activeTaskExecutionRunnable = taskExecutionRunnable;
        taskExecutionRunnable.start();
    }

    @Override
    public void failoverTask() {
        // todo: check if the task can takeover
        ITaskExecutionRunnable failoverTaskExecutionRunnable = taskExecutionRunnableFactory
                .createFailoverTaskExecutionRunnable(activeTaskExecutionRunnable, workflowExecutionContext);
        taskExecutionRunnableMap.put(failoverTaskExecutionRunnable.getIdentify(), failoverTaskExecutionRunnable);
        activeTaskExecutionRunnable = failoverTaskExecutionRunnable;
        failoverTaskExecutionRunnable.start();
    }

    @Override
    public void retryTask() {
        // check the retry times
        ITaskExecutionRunnable retryTaskExecutionRunnable = taskExecutionRunnableFactory
                .createRetryTaskExecutionRunnable(activeTaskExecutionRunnable, workflowExecutionContext);
        taskExecutionRunnableMap.put(retryTaskExecutionRunnable.getIdentify(), retryTaskExecutionRunnable);
        activeTaskExecutionRunnable = retryTaskExecutionRunnable;

        retryTaskExecutionRunnable.start();
    }

    @Override
    public void pauseTask() {
        activeTaskExecutionRunnable.pause();
    }

    @Override
    public void killTask() {
        activeTaskExecutionRunnable.kill();
    }

    @Override
    public ITaskIdentify getTaskIdentify() {
        return taskIdentify;
    }

    @Override
    public ITaskExecutionRunnable getActiveTaskExecutionRunnable() {
        return activeTaskExecutionRunnable;
    }

    @Override
    public ITask getTask() {
        return task;
    }

    @Override
    public List<ITaskExecutionRunnable> getTaskExecutionRunnableList() {
        return null;
    }

    @Override
    public ITaskExecutionRunnable getTaskExecutionRunnable(ITaskExecutionRunnableIdentify taskExecutionRunnableIdentify) {
        return taskExecutionRunnableMap.get(taskExecutionRunnableIdentify);
    }

    @Override
    public ITaskExecutionRunnable storeTaskExecutionRunnable(ITaskExecutionRunnable taskExecutionRunnable) {
        if (taskExecutionRunnable == null) {
            throw new IllegalArgumentException("taskExecutionRunnable cannot be null");
        }
        return taskExecutionRunnableMap.put(taskExecutionRunnable.getIdentify(), taskExecutionRunnable);
    }

}
