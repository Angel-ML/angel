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

/**
 * Get rows use pipeline mode request.
 */
public class GetRowsFlowRequest extends UserRequest {
  /** row indexes */
  private final RowIndex index;

  /**
   * 
   * Create a new GetRowsFlowRequest.
   *
   * @param index row indexes
   * @param clock clock value
   */
  public GetRowsFlowRequest(RowIndex index, int clock) {
    super(UserRequestType.GET_ROWS, clock);
    this.index = index;
  }

  /**
   * Get row indexes
   * 
   * @return RowIndex row indexes
   */
  public RowIndex getIndex() {
    return index;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((index == null) ? 0 : index.hashCode());
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
    GetRowsFlowRequest other = (GetRowsFlowRequest) obj;
    if (index == null) {
      if (other.index != null)
        return false;
    } else if (!index.equals(other.index))
      return false;
    return true;
  }
}
