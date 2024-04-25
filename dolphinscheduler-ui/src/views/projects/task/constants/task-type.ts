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
export type TaskType =
  | 'SHELL'
  | 'SUB_PROCESS'
  | 'PROCEDURE'
  | 'SQL'
  | 'SPARK'
  | 'FLINK'
  | 'PYTHON'
  | 'DEPENDENT'
  | 'HTTP'
  | 'DATAX'
  | 'ADDAX'
  | 'CONDITIONS'
  | 'SWITCH'
  | 'FLINK_STREAM'

export type TaskExecuteType = 'STREAM' | 'BATCH'

export const TASK_TYPES_MAP = {
  SHELL: {
    alias: 'SHELL'
  },
  SUB_PROCESS: {
    alias: 'SUB_PROCESS'
  },
  PROCEDURE: {
    alias: 'PROCEDURE'
  },
  SQL: {
    alias: 'SQL'
  },
  SPARK: {
    alias: 'SPARK'
  },
  FLINK: {
    alias: 'FLINK'
  },
  PYTHON: {
    alias: 'PYTHON'
  },
  DEPENDENT: {
    alias: 'DEPENDENT'
  },
  HTTP: {
    alias: 'HTTP'
  },
  ADDAX: {
    alias: 'Addax'
  },
  CONDITIONS: {
    alias: 'CONDITIONS'
  },
  SWITCH: {
    alias: 'SWITCH'
  },
  FLINK_STREAM: {
    alias: 'FLINK_STREAM',
    helperLinkDisable: true,
    taskExecuteType: 'STREAM'
  },
  HIVECLI: {
    alias: 'HIVECLI',
    helperLinkDisable: true
  },
  DATA_FACTORY: {
    alias: 'DATA_FACTORY',
    helperLinkDisable: true
  }
} as {
    [key in TaskType]: {
      alias: string
      helperLinkDisable?: boolean
      taskExecuteType?: TaskExecuteType
    }
  }
