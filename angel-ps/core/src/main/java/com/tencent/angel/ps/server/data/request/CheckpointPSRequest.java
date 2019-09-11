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
package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;

public class CheckpointPSRequest extends PSRequest {

  private int matrixId;
  private int checkPointId;

  public CheckpointPSRequest(int userRequestId, int matrixId, int checkPointId, ParameterServerId psId) {
    super(userRequestId, psId);
    this.matrixId = matrixId;
    this.checkPointId = checkPointId;
  }

  public CheckpointPSRequest() {
    this(-1, -1, -1, null);
  }

  public int getMatrixId() {
    return matrixId;
  }

  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  public int getCheckPointId() {
    return checkPointId;
  }

  public void setCheckPointId(int checkPointId) {
    this.checkPointId = checkPointId;
  }

  @Override
  public int getEstimizeDataSize() {
    return 0;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(matrixId);
    buf.writeInt(checkPointId);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    matrixId = buf.readInt();
    checkPointId = buf.readInt();
  }

  @Override public int bufferLen() {
    return 4 + 4 + super.bufferLen();
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.CHECKPOINT;
  }

  @Override
  public boolean timeoutEnable() {
    return false;
  }
}
