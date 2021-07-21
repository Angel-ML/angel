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

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleNodeFeat extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNodeFeat(SampleNodeFeatParam param) {
    super(param);
  }

  public SampleNodeFeat() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNodeFeatParam sampleParam = (PartSampleNodeFeatParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    int count = Math.min(row.size(), sampleParam.getCount());
    IntFloatVector[] feats =
        SampleUtils.sampleNodeFeatByCount(row, count, System.currentTimeMillis());

    return new PartSampleNodeFeatResult(sampleParam.getPartKey().getPartitionId(), feats);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<IntFloatVector> results = new ArrayList<>();
    for (PartitionGetResult partResult : partResults) {
      results.addAll(Arrays.asList(((PartSampleNodeFeatResult) partResult).getNodeFeats()));
    }
    return new SampleNodeFeatResult(results);
  }
}