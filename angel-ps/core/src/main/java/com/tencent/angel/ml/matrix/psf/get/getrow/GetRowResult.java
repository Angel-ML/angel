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


package com.tencent.angel.ml.matrix.psf.get.getrow;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;

public class GetRowResult extends GetResult {
  /**
   * matrix row
   */
  private final Vector row;

  /**
   * Create a new GetRowResult.
   *
   * @param type response type
   * @param row  matrix row
   */
  public GetRowResult(ResponseType type, Vector row) {
    super(type);
    this.row = row;
  }

  /**
   * Get matrix row
   *
   * @return Vector matrix row
   */
  public Vector getRow() {
    return row;
  }
}
