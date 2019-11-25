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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.params.UpdateParamWithKeyIds;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.Arrays;

public class PushPathTailParam extends UpdateParamWithKeyIds {
  private Long2LongOpenHashMap pathTail;

  public PushPathTailParam(int matrixId, boolean updateClock, Long2LongOpenHashMap pathTail) {
    super(matrixId, updateClock);
    this.pathTail = pathTail;

    keyIds = pathTail.keySet().toLongArray();
    Arrays.sort(keyIds);
  }

  public PushPathTailParam(int matrixId, Long2LongOpenHashMap pathTail) {
    this(matrixId, false, pathTail);
  }

  @Override
  protected PartitionUpdateParam getPartitionParam(PartitionKey part, int startIdx, int endIdx) {
    return new PushPathTailPartitionParam(matrixId, part, pathTail, keyIds, startIdx, endIdx);
  }
}
