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
package com.tencent.angel.graph.client.node2vec.getfuncs.getmaxdegree;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;


public class PullMaxDegreePartitionResult extends PartitionGetResult {

  private int partResult;

  public PullMaxDegreePartitionResult(int partResult) {
    this.partResult = partResult;
  }

  public PullMaxDegreePartitionResult() {
    super();
  }

  public int getPartResult() {
    return partResult;
  }

  public void setPartResult(int partResult) {
    this.partResult = partResult;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partResult);
  }

  @Override
  public void deserialize(ByteBuf input) {
    partResult = input.readInt();
  }

  @Override
  public int bufferLen() {
    return 4;
  }
}
