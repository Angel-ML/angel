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


package com.tencent.angel.common;

public enum AngelEnvironment {
  PARAMETERSERVER_ID("PARAMETERSERVER_ID"), WORKER_GROUP_ID("WORKER_GROUP_ID"), WORKER_ID(
    "WORKER_ID"), TASK_ID("TASK_ID"), LISTEN_ADDR("LISTEN_ADDR"), LISTEN_PORT(
    "LISTEN_PORT"), PS_ATTEMPT_ID("PS_ATTEMPT_ID"),

  WORKERGROUP_NUMBER("WORKERGROUP_NUMBER"), TASK_NUMBER("TASK_NUMBER"), INIT_MIN_CLOCK(
    "INIT_MIN_CLOCK"), PSAGENT_ID("PSAGENT_ID"), PSAGENT_ATTEMPT_ID(
    "PSAGENT_ATTEMPT_ID"), WORKER_ATTEMPT_ID("WORKER_ATTEMPT_ID"), ANGEL_USER_TASK(
    "ANGEL_USER_TASK");

  private final String variable;

  private AngelEnvironment(String variable) {
    this.variable = variable;
  }

  public String key() {
    return variable;
  }

  public String toString() {
    return variable;
  }
}
