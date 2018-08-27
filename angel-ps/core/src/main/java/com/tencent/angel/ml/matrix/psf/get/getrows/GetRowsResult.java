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


package com.tencent.angel.ml.matrix.psf.get.getrows;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;

import java.util.Map;

/**
 * The final result return to user of the get rows function.
 */
public class GetRowsResult extends GetResult {
  /**
   * matrix row index to row map
   */
  private final Map<Integer, Vector> rows;

  /**
   * Create a new GetRowsResult.
   *
   * @param responseType response type
   * @param rows         matrix rows
   */
  public GetRowsResult(ResponseType responseType, Map<Integer, Vector> rows) {
    super(responseType);
    this.rows = rows;
  }

  /**
   * Get rows.
   *
   * @return Map<Integer, TVector> rows
   */
  public Map<Integer, Vector> getRows() {
    return rows;
  }
}
