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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

import java.util.UUID;

/**
 * Get row request.
 */
public class GetRowRequest extends UserRequest {
  /** matrix id */
  private final int matrixId;

  /** row index */
  private final int rowIndex;

  private final UUID uuid = UUID.randomUUID();

  /**
   * 
   * Create a new GetRowRequest.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param clock clock value
   */
  public GetRowRequest(int matrixId, int rowIndex, int clock) {
    super(UserRequestType.GET_ROW, clock);
    this.matrixId = matrixId;
    this.rowIndex = rowIndex;
  }

  /**
   * Get matrix id.
   * 
   * @return int matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Get row index
   * 
   * @return int row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + matrixId;
    result = prime * result + rowIndex;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    GetRowRequest other = (GetRowRequest) obj;
    if (matrixId != other.matrixId)
      return false;
    return rowIndex == other.rowIndex;
  }

  @Override
  public String toString() {
    return "GetRowRequest [id = " + uuid + ", matrixId=" + matrixId + ", rowIndex=" + rowIndex + ", toString()="
        + super.toString() + "]";
  }
}
