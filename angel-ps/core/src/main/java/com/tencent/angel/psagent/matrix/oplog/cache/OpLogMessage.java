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


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.psagent.task.TaskContext;

public class OpLogMessage {

  private final int matrixId;
  private final OpLogMessageType type;
  private final TaskContext context;
  private final int seqId;

  public OpLogMessage(int seqId, int matrixId, OpLogMessageType type, TaskContext context) {
    this.seqId = seqId;
    this.matrixId = matrixId;
    this.type = type;
    this.context = context;
  }

  public OpLogMessageType getType() {
    return type;
  }

  public TaskContext getContext() {
    return context;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int message() {
    return seqId;
  }

  @Override
  public String toString() {
    return "OpLogMessage [matrixId=" + matrixId + ", type=" + type + ", context=" + context
        + ", seqId=" + seqId + "]";
  }

  public int getSeqId() {
    return seqId;
  }
}
