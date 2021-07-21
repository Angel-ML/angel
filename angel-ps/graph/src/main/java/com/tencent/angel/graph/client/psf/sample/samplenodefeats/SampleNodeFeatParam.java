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

package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class SampleNodeFeatParam extends GetParam {

  /**
   * Sample neighbor number
   */
  protected final int count;

  public SampleNodeFeatParam(int matrixId, int count) {
    super(matrixId);
    this.count = count;
  }

  public SampleNodeFeatParam() {
    this(-1, -1);
  }

  public int getCount() {
    return count;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // sample (count / partNum + 1) in every partition randomly
    int eachSize = count / partitions.length + 1;

    // Generate rpc params
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      partParams.add(new PartSampleNodeFeatParam(matrixId, partitions[i], eachSize));
    }

    return partParams;
  }
}
