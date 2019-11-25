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

package com.tencent.angel.ps.server.data;

import java.util.Objects;

class MatrixPartition {

  private final int matrixId;
  private final int partitionId;

  public MatrixPartition(int matrixId, int partitionId) {
    this.matrixId = matrixId;
    this.partitionId = partitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MatrixPartition)) {
      return false;
    }
    MatrixPartition that = (MatrixPartition) o;
    return getMatrixId() == that.getMatrixId() &&
        getPartitionId() == that.getPartitionId();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMatrixId(), getPartitionId());
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int getPartitionId() {
    return partitionId;
  }
}