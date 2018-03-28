/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.impl;

import com.tencent.angel.ipc.VersionedProtocol;
import com.tencent.angel.protobuf.generated.MasterPSServiceProtos.MasterPSService;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos;

/**
 * The interface parameter server protocol.
 */
public interface PSProtocol extends VersionedProtocol ,MasterPSService.BlockingInterface,
  PSAgentPSServiceProtos.PSAgentPSService.BlockingInterface{

  static long VERSION = 0L;
  /**
   * The interface Async protocol.
   */
  interface AsyncProtocol extends PSProtocol {
  }
}
