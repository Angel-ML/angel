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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.udf.updater;

import java.util.List;

/**
 * The skeleton of updater parameter.
 */
abstract public class UpdaterParam {
  /**
   * The Matrix id.
   */
  protected final int matrixId;
  /**
   * The Update clock.
   */
  protected final boolean updateClock;

  /**
   * Creates a new parameter.
   *
   * @param matrixId    the matrix id
   * @param updateClock the update clock
   */
  public UpdaterParam(int matrixId, boolean updateClock){
    this.matrixId = matrixId;
    this.updateClock = updateClock;
  }

  /**
   * Split list.
   *
   * @return the list
   */
  public abstract List<PartitionUpdaterParam> split();

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Is update clock boolean.
   *
   * @return the boolean
   */
  public boolean isUpdateClock() {
    return updateClock;
  }
}
