/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.webapp;

/**
 * params that the pages can support by http;
 */
public interface AngelParams {
  static final String APP_ID = "app.id";
  static final String WORKERGROUP_ID = "workergroup.id";
  static final String WORKERGROUP_STATE = "workergroup.state";
  // static final String WORKER_ID = "worker.id";
  static final String WORKER_ATTEMPT_ID = "workerattempt.id";
  static final String PARAMETERSERVER_STATE = "parameterserver.state";
  static final String PSATTEMPT_ID = "PSAttempt.id";
  static final String TASK_ID = "task.id";
  static final String PROGRESS="progress";

}
