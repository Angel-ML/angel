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
import com.tencent.angel.ml.math2.storage.IntIntDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.List;
import java.util.Map;

public class IntIntVectorSplitter implements ISplitter {
  @Override
  public Map<PartitionKey, RowUpdateSplit> split(Vector vector, List<PartitionKey> parts) {
    IntIntVectorStorage storage = ((IntIntVector) vector).getStorage();
    if (storage instanceof IntIntDenseVectorStorage) {
      return RowUpdateSplitUtils.split(vector.getRowId(), storage.getValues(), parts);
    } else if (storage instanceof IntIntSparseVectorStorage) {
      return RowUpdateSplitUtils
          .split(vector.getRowId(), storage.getIndices(), storage.getValues(), parts, false);
    } else if (storage instanceof IntIntSortedVectorStorage) {
      return RowUpdateSplitUtils.split(vector.getRowId(), storage.getIndices(), storage.getValues(), parts, true);
    } else {
      throw new UnsupportedOperationException(
          "unsupport split for storage type:" + storage.getClass().getName());
    }
  }
}
