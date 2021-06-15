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
  private final int rowId;


  /**
   * Create a new GetRowRequest.
   *
   * @param matrixId matrix id
   * @param rowId row index
   */
  public GetRowRequest(int matrixId, int rowId) {
    super(UserRequestType.GET_ROW);
    this.matrixId = matrixId;
    this.rowId = rowId;
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
  public int getRowId() {
    return rowId;
  }

  @Override public String toString() {
    return "GetRowRequest{" + "matrixId=" + matrixId + ", rowId=" + rowId + super.toString();
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    GetRowRequest that = (GetRowRequest) o;

    if (matrixId != that.matrixId)
      return false;
    if (rowId != that.rowId)
      return false;

    return true;
  }

  @Override public int hashCode() {
    int result = matrixId;
    result = 31 * result + rowId;
    return result;
  }
}