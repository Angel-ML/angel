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


package com.tencent.angel.psagent.matrix.oplog.cache.splitter;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.vector.CompLongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.oplog.cache.CompLongDoubleRowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompLongDoubleVectorSplitter implements ISplitter {

  @Override
  public Map<PartitionKey, RowUpdateSplit> split(Vector vector, List<PartitionKey> parts) {
    LongDoubleVector[] vecParts = ((CompLongDoubleVector)vector).getPartitions();
    assert vecParts.length == parts.size();

    Map<PartitionKey, RowUpdateSplit> updateSplitMap = new HashMap<>(parts.size());
    for (int i = 0; i < vecParts.length; i++) {
      updateSplitMap.put(parts.get(i),
          new CompLongDoubleRowUpdateSplit(vector.getRowId(), vecParts[i]));
    }
    return updateSplitMap;
  }
}
