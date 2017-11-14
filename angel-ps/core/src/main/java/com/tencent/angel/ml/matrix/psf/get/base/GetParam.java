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

package com.tencent.angel.ml.matrix.psf.get.base;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The Get parameter. The class that extends this interface must have a constructor without
 * parameters.
 */
public class GetParam implements ParamSplit {
  /**
   * The Matrix id.
   */
  public final int matrixId;

  /**
   * Creates a new Get parameter.
   *
   * @param matrixId the matrix id
   */
  public GetParam(int matrixId) {
    this.matrixId = matrixId;
  }

  public GetParam() {
    this(-1);
  }

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (int i = 0; i < size; i++) {
      partParams.add(new PartitionGetParam(matrixId, parts.get(i)));
    }

    return partParams;
  }
}
