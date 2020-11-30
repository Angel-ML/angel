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

package com.tencent.angel.ps.storage.partition;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.StreamSerialize;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partition.storage.IServerPartitionStorage;

public abstract class UserDefinePartition extends ServerPartition implements StreamSerialize {
  /**
   * Create a new Server partition,include load rows.
   *
   * @param partKey the partition meta
   * @param rowType row type
   * @param estSparsity valid element number / index range
   * @param storage partition storage
   */
  public UserDefinePartition(PartitionKey partKey, RowType rowType, double estSparsity,
      IServerPartitionStorage storage) {
    super(partKey, rowType, estSparsity, storage);
  }


  /**
   * Create a new Server partition.
   */
  public UserDefinePartition() {
    this(null, RowType.T_DOUBLE_DENSE, 1.0, null);
  }
}
