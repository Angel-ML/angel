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

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


/**
 * The type Partition key,represent a part of matrix
 *
 */
public class PartitionKey implements Comparable<PartitionKey>, Serialize {

  int partitionId = 0;
  int matrixId = 0;

  /**
   * Elements in this partition row number are in [startRow, endRow) and column number are in
   * [StartCol, endCol);
   */
  int startRow = 0;
  long startCol = 0;
  int endRow = 0;
  long endCol = 0;

  public PartitionKey() {}

  public PartitionKey(int partitionId, int matrixId, int startRow, long startCol, int endRow,
      long endCol) {
    super();
    this.partitionId = partitionId;
    this.matrixId = matrixId;
    this.startRow = startRow;
    this.startCol = startCol;
    this.endRow = endRow;
    this.endCol = endCol;
  }

  public PartitionKey(int matrixId, int partId) {
    this(partId, matrixId, -1, -1, -1, -1);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("PartitionKey(");
    builder.append("matrixId=").append(matrixId).append(", ");
    builder.append("partitionId=").append(partitionId).append(", ");
    builder.append("startRow=").append(startRow).append(", ");
    builder.append("startCol=").append(startCol).append(", ");
    builder.append("endRow=").append(endRow).append(", ");
    builder.append("endCol=").append(endCol).append(")");
    return builder.toString();
  }

  public int getPartitionId() {
    return partitionId;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  public int getStartRow() {
    return startRow;
  }

  public long getStartCol() {
    return startCol;
  }

  public void setStartCol(int startCol) {
    this.startCol = startCol;
  }

  public int getEndRow() {
    return endRow;
  }

  public long getEndCol() {
    return endCol;
  }

  public void setEndCol(int endCol) {
    this.endCol = endCol;
  }

  public void write(DataOutputStream out) throws IOException {
    out.writeInt(startRow);
    out.writeLong(startCol);
    out.writeInt(endRow);
    out.writeLong(endCol);
  }

  public void read(DataInputStream input) throws IOException {
    startRow = input.readInt();
    startCol = input.readLong();
    endRow = input.readInt();
    endCol = input.readLong();
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public void setStartRow(int startRow) {
    this.startRow = startRow;
  }

  public void setEndRow(int endRow) {
    this.endRow = endRow;
  }

  @Override
  public int compareTo(PartitionKey other) {
    if (this.startRow > other.startRow) {
      return 1;
    }

    if (this.startRow < other.startRow) {
      return -1;
    }

    if (this.startCol > other.startCol) {
      return 1;
    }

    if (this.startCol < other.startCol) {
      return -1;
    }

    return 0;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(matrixId);
    buf.writeInt(partitionId);
    buf.writeInt(startRow);
    buf.writeInt(endRow);
    buf.writeLong(startCol);
    buf.writeLong(endCol);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    matrixId = buf.readInt();
    partitionId = buf.readInt();
    startRow = buf.readInt();
    endRow = buf.readInt();
    startCol = buf.readLong();
    endCol = buf.readLong();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + matrixId;
    result = prime * result + partitionId;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PartitionKey other = (PartitionKey) obj;
    if (matrixId != other.matrixId)
      return false;
    return partitionId == other.partitionId;
  }
}
