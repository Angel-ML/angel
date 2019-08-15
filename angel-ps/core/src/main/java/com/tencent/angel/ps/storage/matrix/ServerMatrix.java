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


package com.tencent.angel.ps.storage.matrix;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.partition.IServerPartition;
import com.tencent.angel.ps.storage.partition.storage.IServerPartitionStorage;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.partition.ServerPartitionFactory;
import com.tencent.angel.ps.storage.vector.element.IElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * The Server matrix on parameter server,assigned by {@link com.tencent.angel.master.AngelApplicationMaster},which
 * represents a set of partitions of matrix
 */
public class ServerMatrix {

  private final static Log LOG = LogFactory.getLog(ServerMatrix.class);

  /**
   * Mapping from matrix PartitionKey.partitionId to ServerPartition.
   */
  private final HashMap<Integer, ServerPartition> partitionMaps;

  private final int matrixId;

  private final String matrixName;

  private final PSContext context;

  /**
   * The partitions in this PS
   */
  //private final List<PartitionKey> partitionKeys;

  /**
   * Create a new Server matrix by matrix partition.
   *
   * @param matrixMeta the matrix partition contains a set of partitions, which need to load on
   * Parameter Server
   */
  public ServerMatrix(MatrixMeta matrixMeta, PSContext context) {
    this.context = context;

    LOG.info(
        "Creating a Server Matrix, id: " + matrixMeta.getId() + ", name: " + matrixMeta.getName());
    partitionMaps = new HashMap<>(matrixMeta.getPartitionMetas().size());
    matrixId = matrixMeta.getId();
    matrixName = matrixMeta.getName();
  }

  public void init() {
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    Map<Integer, PartitionMeta> partMetas = matrixMeta.getPartitionMetas();

    String sourceClass = matrixMeta.getAttribute(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS,
        AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);

    // Get server partition class
    Class<? extends IServerPartition> partClass;
    try {
      partClass = matrixMeta.getPartitionClass();

      // If partition class is not set, just use the default partition class
      if (partClass == null) {
        partClass = MatrixConf.DEFAULT_SERVER_PARTITION_CLASS;
      }
    } catch (Throwable e) {
      LOG.fatal("Server partition class failed ", e);
      throw new RuntimeException(e);
    }

    // Get server partition storage class type
    Class<? extends IServerPartitionStorage> storageClass;
    try {
      storageClass = matrixMeta.getPartitionStorageClass();
    } catch (Throwable e) {
      LOG.fatal("Server partition class failed ", e);
      throw new RuntimeException(e);
    }

    RowType rowType = matrixMeta.getRowType();

    // Get value class
    Class<? extends IElement> valueClass = null;
    if (rowType.isCompleType()) {
      try {
        valueClass = matrixMeta.getValueClass();
      } catch (Throwable e) {
        LOG.fatal("Init value class failed ", e);
        throw new RuntimeException(e);
      }

      if (valueClass == null) {
        throw new RuntimeException("Complex type must set value type class!!");
      }
    }

    for (PartitionMeta partMeta : partMetas.values()) {
      ServerPartition part = ServerPartitionFactory
          .getPartition(partMeta.getPartitionKey(), partClass, storageClass,
              matrixMeta.getRowType(), valueClass,
              matrixMeta.getEstSparsity());
      partitionMaps.put(partMeta.getPartId(), part);
      part.init();
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  /**
   * Gets partition specified by partition id
   *
   * @param partId the part id
   * @return the partition
   */
  public ServerPartition getPartition(int partId) {
    return partitionMaps.get(partId);
  }

  /**
   * Gets the matrix name.
   *
   * @return the name
   */
  public String getName() {
    return matrixName;
  }


  /**
   * Gets the matrix id.
   *
   * @return the id
   */
  public int getId() {
    return matrixId;
  }

  /**
   * Get all partitions in this ServerMatrix
   *
   * @return all partitions in this ServerMatrix
   */
  public Map<Integer, ServerPartition> getPartitions() {
    return partitionMaps;
  }


  public void startServering() {
    for (ServerPartition part : partitionMaps.values()) {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }
}
