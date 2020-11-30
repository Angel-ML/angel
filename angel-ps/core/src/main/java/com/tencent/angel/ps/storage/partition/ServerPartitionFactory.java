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
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partition.storage.CSRStorage;
import com.tencent.angel.ps.storage.partition.storage.DenseServerRowsStorage;
import com.tencent.angel.ps.storage.partition.storage.IServerPartitionStorage;
import com.tencent.angel.ps.storage.partition.storage.ServerPartitionStorageFactory;
import com.tencent.angel.ps.storage.partition.storage.ServerRowsStorage;
import com.tencent.angel.ps.storage.vector.element.IElement;

/**
 * Server Partition factory
 */
public class ServerPartitionFactory {

  /**
   * Create a new server partition
   *
   * @param partKey partition key
   * @param partClass partition class
   * @param storageClass storage class
   * @param rowType row type
   * @param valueClass value class
   * @param estSparsity estimate sparsity
   * @return server partition
   */
  public static ServerPartition getPartition(PartitionKey partKey,
      Class<? extends IServerPartition> partClass,
      Class<? extends IServerPartitionStorage> storageClass,
      RowType rowType, Class<? extends IElement> valueClass, double estSparsity) {
    // Row base partition
    if (partClass == RowBasedPartition.class) {
      // If storage class is not set, use default DenseServerRowsStorage
      if (storageClass == null) {
        storageClass = DenseServerRowsStorage.class;
      }

      try {
        ServerRowsStorage storage = ServerPartitionStorageFactory
            .getRowBasedStorage(storageClass, partKey, rowType);
        return new RowBasedPartition(partKey, estSparsity, storage, rowType, valueClass);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    } else if (partClass == CSRPartition.class) {
      CSRStorage storage = ServerPartitionStorageFactory
          .getCSRStorage(storageClass, partKey, rowType);
      return new CSRPartition(partKey, estSparsity, storage, rowType);
    } else {
      try {
        ServerPartition part = (ServerPartition) partClass.newInstance();
        part.setPartitionKey(partKey);
        return part;
      } catch (Throwable e) {
        throw new UnsupportedOperationException("can not load partition class ", e);
      }
    }
  }

  /**
   * Create a new empty server partition
   *
   * @param partClassName partition class
   * @return server partition
   */
  public static ServerPartition getPartition(String partClassName) {
    if (RowBasedPartition.class.getName().equals(partClassName)) {
      return new RowBasedPartition();
    } else if (CSRPartition.class.getName().equals(partClassName)) {
      return new CSRPartition();
    } else {
      throw new UnsupportedOperationException(
          "not support partition class " + partClassName + " now ");
    }
  }
}
