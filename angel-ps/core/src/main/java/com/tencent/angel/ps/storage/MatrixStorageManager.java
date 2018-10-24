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


package com.tencent.angel.ps.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.io.load.SnapshotRecover;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The matrix partitions manager on the parameter server.
 */
public class MatrixStorageManager {
  private final static Log LOG = LogFactory.getLog(MatrixStorageManager.class);
  /**
   * matrixId->Matrix
   */
  private final ConcurrentHashMap<Integer, ServerMatrix> matrixIdToDataMap;

  private final PSContext context;

  private final SnapshotRecover recover;

  /**
   * Create a new Matrix partition manager.
   */
  public MatrixStorageManager(PSContext context) {
    this.context = context;
    matrixIdToDataMap = new ConcurrentHashMap<>();
    recover = new SnapshotRecover(context);
  }

  /**
   * Get matrix use matrix id
   *
   * @param matrixId matrix id
   * @return ServerMatrix matrix
   */
  public ServerMatrix getMatrix(int matrixId) {
    return matrixIdToDataMap.get(matrixId);
  }

  /**
   * Add a batch of matrices to parameter server
   *
   * @param matrixMetas matrices meta
   * @throws IOException
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) throws IOException {
    int size = matrixMetas.size();
    for (int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /**
   * Add a matrixto parameter server.
   *
   * @param matrixMeta the matrix partitions
   * @throws IOException load matrix partition from files failed
   */
  public void addMatrix(MatrixMeta matrixMeta) throws IOException {
    int matrixId = matrixMeta.getId();
    if (matrixIdToDataMap.containsKey(matrixId)) {
      LOG.warn("MatrixId [" + matrixId + "] has already been added.");
      return;
    }
    ServerMatrix serverMatrix = new ServerMatrix(matrixMeta, context);
    serverMatrix.init();
    matrixIdToDataMap.put(matrixId, serverMatrix);
    LOG.info("MatrixId [" + matrixId + "] added.");
  }


  /**
   * Remove matrices from parameter server.
   *
   * @param needReleaseMatrices the release matrices
   */
  public void removeMatrices(List<Integer> needReleaseMatrices) {
    int size = needReleaseMatrices.size();
    for (int i = 0; i < size; i++) {
      removeMatrix(needReleaseMatrices.get(i));
    }
  }

  /**
   * Remove matrix from parameter server
   *
   * @param matrixId
   */
  public void removeMatrix(int matrixId) {
    matrixIdToDataMap.remove(matrixId);
  }

  /**
   * Get a row split
   *
   * @param matrixId the matrix id
   * @param rowId    the row id
   * @param partId   the partition ids
   * @return the row if exists, else null
   */
  public ServerRow getRow(int matrixId, int rowId, int partId) {
    ServerMatrix matrix = matrixIdToDataMap.get(matrixId);
    if (matrix != null) {
      return matrix.getRow(partId, rowId);
    } else {
      return null;
    }
  }

  /**
   * Get a row split
   *
   * @param partKey partition key
   * @param rowId   row index
   * @return the row if exists, else null
   */
  public ServerRow getRow(PartitionKey partKey, int rowId) {
    return getRow(partKey.getMatrixId(), rowId, partKey.getPartitionId());
  }

  /**
   * Gets server partition of matrix by partition key
   *
   * @param partKey the partition key
   * @return the server partition if exists,else null
   */
  public ServerPartition getPart(PartitionKey partKey) {
    ServerMatrix matrix = matrixIdToDataMap.get(partKey.getMatrixId());
    if (matrix != null) {
      return matrix.getPartition(partKey.getPartitionId());
    } else {
      return null;
    }
  }

  /**
   * Gets server partition of matrix by partition key
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return the server partition if exists,else null
   */
  public ServerPartition getPart(int matrixId, int partId) {
    ServerMatrix matrix = matrixIdToDataMap.get(matrixId);
    if (matrix != null) {
      return matrix.getPartition(partId);
    } else {
      return null;
    }
  }

  /**
   * Clear matrices on parameter server
   */
  public void clear() {
    matrixIdToDataMap.clear();
  }

  public ConcurrentHashMap<Integer,ServerMatrix> getMatrices() {
    return matrixIdToDataMap;
  }
}
