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
 *
 */

package com.tencent.angel.ml.matrix.psf.get.multi;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;

import java.util.Map;

/**
 * The final result return to user of the get rows function.
 */
public class GetRowsResult extends GetResult {
  /** matrix row index to row map */
  private final Map<Integer, TVector> rows;

  /**
   * Create a new GetRowsResult.
   *
   * @param responseType response type
   * @param rows matrix rows
   */
  public GetRowsResult(ResponseType responseType, Map<Integer, TVector> rows) {
    super(responseType);
    this.rows = rows;
  }

  /**
   * Get rows.
   *
   * @return Map<Integer, TVector> rows
   */
  public Map<Integer, TVector> getRows() {
    return rows;
  }
}
