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
package com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayIntElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;

public class PullMinDegreeV2 extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullMinDegreeV2(GetParam param) {
    super(param);
  }

  public PullMinDegreeV2() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    //System.out.println("before ServerLongAnyRow Get");
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(partParam.getPartKey(), 0);
    //System.out.println("after ServerLongAnyRow Get");
    int partResult = Integer.MAX_VALUE;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    //System.out.println("before while iter.hasNext");
    while (iter.hasNext()) {
      //System.out.println("enter while iter.hasNext and before entry");
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      /* 原代码
      LongArrayElement value = (LongArrayElement) entry.getValue();
      int length = value.getData().length;
      */
      //对neighbor集分割
      //System.out.println("after entry and before value");
      LongArrayIntElement value = (LongArrayIntElement) entry.getValue();
      //System.out.println("after value ");
      //System.out.println("value is null or not: " + (value == null));
      //System.out.println("get numPart: " + value.getPartNum());
      //System.out.println("get length: " + value.getData());
      int length = 0;
      assert (value.getData().length == value.getPartNum());
      for (int i = 0; i < value.getPartNum(); i++) {
        length += value.getData()[i].length;
      }
      if (length < partResult) {
        partResult = length;
      }
    }

    return new PullMinDegreePartitionResult(partResult);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int result = Integer.MAX_VALUE;

    for (PartitionGetResult partResult : partResults) {
      int part = ((PullMinDegreePartitionResult) partResult).getPartResult();
      if (part < result) {
        result = part;
      }
    }

    return new PullMinDegreeResult(result);
  }
}
