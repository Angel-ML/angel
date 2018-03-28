/*
 * Tencent is pleased to support the open source community by making Angel available.
  *
  * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
  *
  * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
  * compliance with the License. You may obtain a copy of the License at
  *
  * https://opensource.org/licenses/BSD-3-Clause
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is
  * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
  * either express or implied. See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.tencent.angel.ps.impl;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.MatrixMetaManager;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ps.ParameterServerId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PS Matrix meta manager
 */
public class PSMatrixMetaManager {
  /**
   * Matrix meta manager
   */
  private final MatrixMetaManager matrixMetaManager;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create a PSMatrixMetaManager
   * @param context PS context
   */
  public PSMatrixMetaManager(PSContext context) {
    this.context = context;
    this.matrixMetaManager = new MatrixMetaManager();
  }

  /**
   * Add matrices meta
   * @param matrices matrices meta
   */
  public void addMatrices(List<MatrixMeta> matrices) {
    matrixMetaManager.addMatrices(matrices);
  }

  /**
   * Add matrix meta
   * @param matrix matrix meta
   */
  public void addMatrix(MatrixMeta matrix) {
    matrixMetaManager.addMatrix(matrix);
  }

  /**
   * Remove matrices meta
   * @param matrixIds matrices id
   */
  public void removeMatrices(List<Integer> matrixIds) {
    matrixMetaManager.removeMatrices(matrixIds);
  }

  /**
   * Get matrix meta
   * @param matrixId matrix id
   * @return matrix meta
   */
  public MatrixMeta getMatrixMeta(int matrixId) {
    return matrixMetaManager.getMatrixMeta(matrixId);
  }

  /**
   * Get matrix meta
   * @param name matrix name
   * @return matrix meta
   */
  public MatrixMeta getMatrixMeta(String name) {
    return matrixMetaManager.getMatrixMeta(name);
  }

  /**
   * Get all matrices meta
   * @return all matrices meta
   */
  public Map<Integer,MatrixMeta> getMatrixMetas() {
    return matrixMetaManager.getMatrixMetas();
  }

  /**
   * Get partition meta
   * @param matrixId matrix id
   * @param partId partition id
   * @return partition meta
   */
  public PartitionMeta getPartMeta(int matrixId, int partId) {
    MatrixMeta meta = getMatrixMeta(matrixId);
    if(meta == null) {
      return null;
    }
    return meta.getPartitionMeta(partId);
  }

  /**
   * Get the server that hold the partition.
   *
   * @param partitionKey matrix partition key
   * @param sync true means get from master, false means get from local cache
   * @return ParameterServerId server id
   */
  public List<ParameterServerId> getPss(PartitionKey partitionKey, boolean sync) throws
    ServiceException {
    if(!sync) {
      return getPss(partitionKey);
    } else {
      List<ParameterServerId> psIds = context.getMaster().getStoredPss(
        partitionKey.getMatrixId(), partitionKey.getPartitionId());
      matrixMetaManager.setPss(partitionKey.getMatrixId(), partitionKey.getPartitionId(), psIds);
      return psIds;
    }
  }

  /**
   * Get partition location: includes stored pss and the location of the pss
   * @param partitionKey partition information
   * @param sync true means get from master; false means get from cache
   * @return partition location
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(PartitionKey partitionKey, boolean sync) throws ServiceException {
    if(!sync) {
      return getPartLocation(partitionKey);
    } else {
      return context.getMaster().getPartLocation(partitionKey.getMatrixId(), partitionKey.getPartitionId());
    }
  }

  /**
   * Get partition location: includes stored pss and the location of the pss
   * @param partitionKey partition information
   * @return partition location
   * @throws ServiceException
   */
  public PartitionLocation getPartLocation(PartitionKey partitionKey) throws ServiceException {
    List<ParameterServerId> psIds = getPss(partitionKey);
    if(psIds == null) {
      return new PartitionLocation(new ArrayList<>());
    }
    int size = psIds.size();
    List<PSLocation> psLocs = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      psLocs.add(new PSLocation(psIds.get(i), context.getLocationManager().getPsLocation(psIds.get(i))));
    }
    return new PartitionLocation(psLocs);
  }

  /**
   * Get the server that hold the partition.
   *
   * @param partitionKey matrix partition key
   * @return ParameterServerId server id
   */
  public List<ParameterServerId> getPss(PartitionKey partitionKey) {
    return matrixMetaManager.getPss(partitionKey.getMatrixId(), partitionKey.getPartitionId());
  }
}
