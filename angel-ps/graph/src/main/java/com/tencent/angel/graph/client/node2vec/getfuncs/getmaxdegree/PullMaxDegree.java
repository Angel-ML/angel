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
package com.tencent.angel.graph.client.node2vec.getfuncs.getmaxdegree;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;

public class PullMaxDegree extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullMaxDegree(GetParam param) {
    super(param);
  }

  public PullMaxDegree() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(partParam.getPartKey(), 0);

    int partResult = Integer.MIN_VALUE;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      LongArrayElement value = (LongArrayElement) entry.getValue();
      int length = value.getData().length;

      if (length > partResult) {
        partResult = length;
      }
    }

    return new PullMaxDegreePartitionResult(partResult);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int result = Integer.MIN_VALUE;

    for (PartitionGetResult partResult : partResults) {
      int part = ((PullMaxDegreePartitionResult) partResult).getPartResult();
      if (part > result) {
        result = part;
      }
    }

    return new PullMaxDegreeResult(result);
  }
}
