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
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import java.util.List;
import java.util.Map;

/**
 * Vector split interface
 */
public interface ISplitter {

  /**
   * Split the vector to ps vector splits
   *
   * @param vector the vector need split
   * @param parts the ps partitions that contains this row, the partitions are sorted by start
   * column index
   * @return ps vector list
   */
  Map<PartitionKey, RowUpdateSplit> split(Vector vector, List<PartitionKey> parts);
}
