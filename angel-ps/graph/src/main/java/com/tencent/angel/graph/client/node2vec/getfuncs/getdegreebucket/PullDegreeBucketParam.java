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
package com.tencent.angel.graph.client.node2vec.getfuncs.getdegreebucket;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class PullDegreeBucketParam extends GetParam {
  private int maxDegree;
  private int minDegree;
  private int numBuckets;

  public PullDegreeBucketParam(int matrixId, int maxDegree, int minDegree, int numBuckets) {
    super(matrixId);
    this.maxDegree = maxDegree;
    this.minDegree = minDegree;
    this.numBuckets = numBuckets;
  }

  public PullDegreeBucketParam() { super(); }

  public int getMaxDegree() {
    return maxDegree;
  }

  public void setMaxDegree(int maxDegree) {
    this.maxDegree = maxDegree;
  }

  public int getMinDegree() {
    return minDegree;
  }

  public void setMinDegree(int minDegree) {
    this.minDegree = minDegree;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @Override public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : parts) {
      partParams.add(new PullDegreeBucketPartitionParam(matrixId, part, maxDegree, minDegree, numBuckets));
    }

    return partParams;
  }
}
