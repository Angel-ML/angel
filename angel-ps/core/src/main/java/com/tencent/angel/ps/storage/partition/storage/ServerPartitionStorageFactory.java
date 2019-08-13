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

package com.tencent.angel.ps.storage.partition.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.utils.RowType;

/**
 * Server partition storage factory
 */
public class ServerPartitionStorageFactory {

  /**
   * Get partition storage use storage class name
   *
   * @param storageClassName storage class name
   * @return partition storage
   */
  public static IServerPartitionStorage getStorage(String storageClassName) {
    try {
      return (IServerPartitionStorage) Class.forName(storageClassName).newInstance();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get storage for row based partition
   *
   * @param storageClass storage class
   * @param partKey partition key
   * @param rowType row type
   * @return storage for row based partition
   */
  public static ServerRowsStorage getRowBasedStorage(
      Class<? extends IServerPartitionStorage> storageClass,
      PartitionKey partKey, RowType rowType) {
    // If storage is set, just get a instance
    if (storageClass != null) {
      try {
        return (ServerRowsStorage) storageClass
            .getConstructor(int.class, int.class)
            .newInstance(partKey.getStartRow(), partKey.getEndRow() - partKey.getStartRow());
      } catch (Throwable e) {
        throw new RuntimeException("get storage failed: ", e);
      }
    }

    // If storage is not set, use default
    return new DenseServerRowsStorage(partKey.getStartRow(),
        partKey.getEndRow() - partKey.getStartRow());
  }

  /**
   * Get storage for CSR partition
   *
   * @param storageClass storage class
   * @param partKey partition key
   * @param rowType row type
   * @return storage for CSR partition
   */
  public static CSRStorage getCSRStorage(Class<? extends IServerPartitionStorage> storageClass,
      PartitionKey partKey, RowType rowType) {
    // If storage is set, just get a instance
    if (storageClass != null) {
      try {
        return (CSRStorage) storageClass
            .getConstructor(int.class)
            .newInstance(partKey.getStartRow());
      } catch (Throwable e) {
        throw new RuntimeException("get storage failed: ", e);
      }
    }

    // If storage is not set, use default
    if (rowType.isInt()) {
      return new IntCSRStorage(partKey.getStartRow());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport row type " + rowType + " for CSR format now");
    }
  }
}
