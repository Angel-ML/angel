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


package com.tencent.angel.ps.ha;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ps.server.data.request.PartitionRequest;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import io.netty.buffer.ByteBuf;

/**
 * PS to PS push interface, it is used to back up matrix partition update use "PUSH" mode
 */
public interface PS2PSPusher {
  /**
   * Push the update to the slave pss
   *
   * @param request update request
   * @param msg     update data
   * @param partLoc the slaves pss
   */
  void put(PartitionRequest request, ByteBuf msg, PartitionLocation partLoc);

  /**
   * Update the clock vector in slave pss
   *
   * @param partKey   partition information
   * @param taskIndex task index
   * @param clock     clock value
   * @param partLoc   the pss that the partition is stored
   */
  void updateClock(PartitionKey partKey, int taskIndex, int clock, PartitionLocation partLoc);

  /**
   * Recover a partition to a slave ps
   *
   * @param part partition recover information: partition id, the slave ps id, the slave ps location
   */
  FutureResult<Response> recover(RecoverPartKey part);
}
