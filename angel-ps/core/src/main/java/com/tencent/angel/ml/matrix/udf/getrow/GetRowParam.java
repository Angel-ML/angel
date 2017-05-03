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

package com.tencent.angel.ml.matrix.udf.getrow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The Get row parameter.
 */
public class GetRowParam implements ParamSplit{
  /**
   * The Matrix id.
   */
  protected final int matrixId;
  /**
   * The Row index.
   */
  protected final int rowIndex;
  /**
   * The Clock.
   */
  protected int clock;
  /**
   * The Bypass mode.
   */
  protected final boolean bypassMode;

  /**
   * Creates a new parameter.
   *
   * @param matrixId   the matrix id
   * @param rowIndex   the row index
   * @param clock      the clock
   * @param bypassMode the bypass mode
   */
  public GetRowParam(int matrixId, int rowIndex, int clock, boolean bypassMode){
    this.matrixId = matrixId;
    this.rowIndex = rowIndex;
    this.clock = clock;
    this.bypassMode = bypassMode;
  }

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Gets clock.
   *
   * @return the clock
   */
  public int getClock() {
    return clock;
  }

  /**
   * Is bypass mode boolean.
   *
   * @return the boolean
   */
  public boolean isBypassMode() {
    return bypassMode;
  }

  /**
   * Gets row index.
   *
   * @return the row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override
  public List<PartitionGetRowParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionGetRowParam> partParams = new ArrayList<PartitionGetRowParam>(size);

    for (int i = 0; i < size; i++) {
      partParams.add(new PartitionGetRowParam(matrixId, parts.get(i), rowIndex, clock, false));
    }

    return partParams;
  }

  /**
   * Sets clock.
   *
   * @param clock the clock
   */
  public void setClock(int clock) {
    this.clock = clock;
  }
}
