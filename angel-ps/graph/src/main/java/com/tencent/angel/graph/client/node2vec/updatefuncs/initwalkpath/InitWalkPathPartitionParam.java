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
import io.netty.buffer.ByteBuf;

public class InitWalkPathPartitionParam extends PartitionUpdateParam {

  private int neighborMatrixId;
  private int walkLength;
  private int numParts;
  private int threshold;
  private double keepProba;
  private boolean isTrunc;

  public InitWalkPathPartitionParam(int matrixId, PartitionKey partKey, boolean updateClock,
      int neighborMatrixId, int walkLength, int numParts,
      int threshold, double keepProba, boolean isTrunc) {
    super(matrixId, partKey, updateClock);
    this.neighborMatrixId = neighborMatrixId;
    this.walkLength = walkLength;
    this.numParts = numParts;
    this.threshold = threshold;
    this.keepProba = keepProba;
    this.isTrunc = isTrunc;
  }

  public InitWalkPathPartitionParam(int matrixId, PartitionKey partKey, int neighborMatrixId,
      int walkLength, int numParts,
      int threshold, double keepProba, boolean isTrunc) {
    this(matrixId, partKey, false, neighborMatrixId, walkLength, numParts, threshold, keepProba,
        isTrunc);
  }

  public InitWalkPathPartitionParam(int matrixId, PartitionKey partKey, int neighborMatrixId,
      int walkLength, int numParts) {
    this(matrixId, partKey, false, neighborMatrixId, walkLength, numParts, -1, 1.0, false);
  }

  public InitWalkPathPartitionParam() {

  }

  public int getNeighborMatrixId() {
    return neighborMatrixId;
  }

  public int getWalkLength() {
    return walkLength;
  }

  public int getThreshold() {
    return threshold;
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }

  public double getKeepProba() {
    return keepProba;
  }

  public void setKeepProba(double keepProba) {
    this.keepProba = keepProba;
  }

  public int getNumParts() {
    return numParts;
  }

  public void setNumParts(int numParts) {
    this.numParts = numParts;
  }

  public boolean isTrunc() {
    return isTrunc;
  }

  public void setTrunc(boolean trunc) {
    isTrunc = trunc;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(neighborMatrixId);
    buf.writeInt(walkLength);
    buf.writeInt(numParts);
    buf.writeInt(threshold);
    buf.writeDouble(keepProba);
    if (isTrunc) {
      buf.writeInt(1);
    } else {
      buf.writeInt(0);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    neighborMatrixId = buf.readInt();
    walkLength = buf.readInt();
    numParts = buf.readInt();
    threshold = buf.readInt();
    keepProba = buf.readDouble();
    isTrunc = buf.readInt() == 1;
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    return len + 28;
  }
}
