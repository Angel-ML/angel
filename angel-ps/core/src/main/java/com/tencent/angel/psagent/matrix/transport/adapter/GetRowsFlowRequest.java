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
 * Get rows use pipeline mode request.
 */
public class GetRowsFlowRequest extends UserRequest {
  /**
   * row indexes
   */
  private final RowIndex index;

  /**
   * Local matrix clock
   */
  private final int clock;

  /**
   * Create a new GetRowsFlowRequest.
   *
   * @param index row indexes
   * @param clock clock value
   */
  public GetRowsFlowRequest(RowIndex index, int clock) {
    super(UserRequestType.GET_ROWS);
    this.index = index;
    this.clock = clock;
  }

  /**
   * Get row indexes
   *
   * @return RowIndex row indexes
   */
  public RowIndex getIndex() {
    return index;
  }

  /**
   * Get matrix clock
   *
   * @return matrix clock
   */
  public int getClock() {
    return clock;
  }
}
