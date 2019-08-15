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
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.List;
import java.util.Map;

/**
 * Long key long value vector splitter
 */
public class LongLongVectorSplitter implements ISplitter {

  @Override
  public Map<PartitionKey, RowUpdateSplit> split(Vector vector, List<PartitionKey> parts) {
    LongLongVectorStorage storage = ((LongLongVector) vector).getStorage();
    if (storage instanceof LongLongSparseVectorStorage) {
      return RowUpdateSplitUtils
          .split(vector.getRowId(), storage.getIndices(), storage.getValues(), parts, false);
    } else if (storage instanceof LongLongSortedVectorStorage) {
      return RowUpdateSplitUtils
          .split(vector.getRowId(), storage.getIndices(), storage.getValues(), parts, true);
    } else {
      throw new UnsupportedOperationException(
          "unsupport split for storage type:" + storage.getClass().getName());
    }
  }
}
