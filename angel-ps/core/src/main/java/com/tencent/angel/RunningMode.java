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


package com.tencent.angel;

/**
 * The enum Running mode.
 *
 * @see com.tencent.angel.master.AngelApplicationMaster
 * @see com.tencent.angel.psagent.PSAgent
 * @see com.tencent.angel.worker.Worker
 */
public enum RunningMode {
  /**
   * Only Run ParameterServer
   */
  ANGEL_PS,

  /**
   * Run ParameterServer & Worker(embedded ParameterServerAgent)
   */
  ANGEL_PS_WORKER, ANGEL_LOCAL
}
