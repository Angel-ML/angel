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


package com.tencent.angel.psagent.matrix.transport.adapter;

/**
 * Get row request.
 */
public class GetRowRequest extends UserRequest {
  /**
   * matrix id
   */
  private final int matrixId;

  /**
   * row index
   */
  private final int rowIndex;

  /**
   * Matrix local clock
   */
  private final int clock;

  /**
   * Create a new GetRowRequest.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param clock    clock value
   */
  public GetRowRequest(int matrixId, int rowIndex, int clock) {
    super(UserRequestType.GET_ROW);
    this.matrixId = matrixId;
    this.rowIndex = rowIndex;
    this.clock = clock;
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

  @Override public String toString() {
    return "GetRowRequest{" + "matrixId=" + matrixId + ", rowIndex=" + rowIndex + ", clock=" + clock
      + "} " + super.toString();
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    GetRowRequest that = (GetRowRequest) o;

    if (matrixId != that.matrixId)
      return false;
    if (rowIndex != that.rowIndex)
      return false;
    return clock == that.clock;
  }

  @Override public int hashCode() {
    int result = matrixId;
    result = 31 * result + rowIndex;
    result = 31 * result + clock;
    return result;
  }
}