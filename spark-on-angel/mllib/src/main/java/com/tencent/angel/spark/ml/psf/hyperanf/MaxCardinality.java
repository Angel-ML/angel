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
package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MaxCardinality extends UnaryAggrFunc {

  public MaxCardinality(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public MaxCardinality() {
    super(-1, -1);
  }

  @Override
  public double processRow(ServerRow row) {
    double maxCardinality = 0;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = ((ServerLongAnyRow) row).iterator();
    while (it.hasNext()) {
      maxCardinality = Math.max(maxCardinality, ((HyperLogLogPlusElement)it.next().getValue()).getCardinality());
    }
    return maxCardinality;
  }

  @Override
  public double mergeOp(double a, double b) {
    return Math.max(a, b);
  }

  @Override
  public double mergeInit() {
    return 0;
  }
}
