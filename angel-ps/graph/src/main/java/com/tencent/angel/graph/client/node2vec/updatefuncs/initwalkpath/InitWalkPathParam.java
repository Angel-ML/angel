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
package com.tencent.angel.graph.client.node2vec.updatefuncs.initwalkpath;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class InitWalkPathParam extends UpdateParam {
  private int neighborMatrixId;
  private int walkLength;
  private int mod;

  public InitWalkPathParam(int matrixId, boolean updateClock, int neighborMatrixId, int walkLength, int mod) {
    super(matrixId, updateClock);
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.mod = mod;
  }

  public InitWalkPathParam(int matrixId, int neighborMatrixId, int walkLength, int mod) {
    super(matrixId);
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.mod = mod;
  }

  public InitWalkPathParam(int matrixId) {
    super(matrixId, false);
  }

  public int getNeighborMatrixId() {
    return neighborMatrixId;
  }

  public void setNeighborMatrixId(int neighborMatrixId) {
    this.neighborMatrixId = neighborMatrixId;
  }

  public int getWalkLength() {
    return walkLength;
  }

  public void setWalkLength(int walkLength) {
    this.walkLength = walkLength;
  }

  public int getMod() {
    return mod;
  }

  public void setMod(int mod) {
    this.mod = mod;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionUpdateParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : parts) {
      partParams.add(new InitWalkPathPartitionParam(matrixId, part, neighborMatrixId, walkLength, mod));
    }

    return partParams;
  }
}
