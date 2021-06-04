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


package com.tencent.angel.ml.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.storage.matrix.PSMatrixInit;
import com.tencent.angel.ps.storage.partition.IServerPartition;
import com.tencent.angel.ps.storage.partition.storage.IServerPartitionStorage;
import com.tencent.angel.ps.storage.partitioner.HashPartitioner;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyHash;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The meta of matrix.
 */
public class MatrixMeta {
  private final static Log LOG = LogFactory.getLog(MatrixMeta.class);
  private final int totalPartNum;

  /**
   * Matrix basic parameters
   */
  private final MatrixContext matrixContext;

  /**
   * Matrix partitions parameters
   */
  private final Map<Integer, PartitionMeta> partitionMetas;

  /**
   * Partition keys sorted by partition id
   */
  private final PartitionKey[] partitionKeys;

  /**
   * Create a MatrixMeta
   *
   * @param mContext matrix context
   */
  public MatrixMeta(int totalPartNum, MatrixContext mContext) {
    this(totalPartNum, mContext, new HashMap<>());
  }

  /**
   * Create a MatrixMeta
   *
   * @param matrixContext matrix context
   * @param partitionMetas matrix partitions meta
   */
  public MatrixMeta(int totalPartNum, MatrixContext matrixContext,
      Map<Integer, PartitionMeta> partitionMetas) {
    this.totalPartNum = totalPartNum;
    this.matrixContext = matrixContext;
    this.partitionMetas = partitionMetas;
    this.partitionKeys = new PartitionKey[partitionMetas.size()];
    int index = 0;
    for (Entry<Integer, PartitionMeta> partitionMeta : partitionMetas.entrySet()) {
      partitionKeys[index++] = partitionMeta.getValue().getPartitionKey();
    }
  }

  /**
   * Get matrix id
   *
   * @return the id
   */
  public int getId() {
    return matrixContext.getMatrixId();
  }

  /**
   * Gets row num.
   *
   * @return the row num
   */
  public int getRowNum() {
    return matrixContext.getRowNum();
  }

  /**
   * Gets col num.
   *
   * @return the col num
   */
  public long getColNum() {
    return matrixContext.getColNum();
  }

  /**
   * Get number of non-zero elements
   *
   * @return number of non-zero elements
   */
  public long getValidIndexNum() {
    return matrixContext.getValidIndexNum();
  }

  /**
   * Get number of non-zero elements
   *
   * @return number of non-zero elements
   */
  public long getValidIndexNumInOnePart() {
    LOG.info("====valid index number = " + matrixContext.getValidIndexNum() + ", total part num = " + getTotalPartNum());
    return (long) ((double) matrixContext.getValidIndexNum() / getTotalPartNum());
  }

  /**
   * get index range start
   */
  public long getIndexStart() {
    return matrixContext.getIndexStart();
  }

  /**
   * Get index range end
   *
   * @return index range end
   */
  public long getIndexEnd() {
    return matrixContext.getIndexEnd();
  }

  /**
   * Gets name.
   *
   * @return the name
   */
  public String getName() {
    return matrixContext.getName();
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public RowType getRowType() {
    return matrixContext.getRowType();
  }

  /**
   * Gets attribute.
   *
   * @param key the key
   * @param value the default value
   * @return the attribute
   */
  public String getAttribute(String key, String value) {
    if (!matrixContext.getAttributes().containsKey(key)) {
      return value;
    }
    return matrixContext.getAttributes().get(key);
  }

  /**
   * Gets attribute.
   *
   * @param key the key
   * @return the attribute
   */
  public String getAttribute(String key) {
    return matrixContext.getAttributes().get(key);
  }

  /**
   * Is average.
   *
   * @return the result
   */
  public boolean isAverage() {
    String average = getAttribute(MatrixConf.MATRIX_AVERAGE, MatrixConf.DEFAULT_MATRIX_AVERAGE);
    return Boolean.parseBoolean(average);
  }

  /**
   * Is hogwild.
   *
   * @return the result
   */
  public boolean isHogwild() {
    String hogwild = getAttribute(MatrixConf.MATRIX_HOGWILD, MatrixConf.DEFAULT_MATRIX_HOGWILD);
    return Boolean.parseBoolean(hogwild);
  }

  /**
   * Gets staleness.
   *
   * @return the staleness
   */
  public int getStaleness() {
    return Integer.parseInt(getAttribute(MatrixConf.MATRIX_STALENESS, "0"));
  }

  /**
   * Get partitions meta
   *
   * @return all partitions meta
   */
  public Map<Integer, PartitionMeta> getPartitionMetas() {
    return partitionMetas;
  }

  /**
   * Get matrix context
   *
   * @return matrix context
   */
  public MatrixContext getMatrixContext() {
    return matrixContext;
  }

  /**
   * Add meta for a partition
   *
   * @param id partition id
   * @param meta partition meta
   */
  public void addPartitionMeta(int id, PartitionMeta meta) {
    partitionMetas.put(id, meta);
  }

  /**
   * Get meta for a partition
   *
   * @param partId partition id
   * @return partition meta
   */
  public PartitionMeta getPartitionMeta(int partId) {
    return partitionMetas.get(partId);
  }

  /**
   * Get the stored pss for a partition
   *
   * @param partId partition id
   * @return the stored pss
   */
  public List<ParameterServerId> getPss(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return null;
    }
    return partitionMeta.getPss();
  }

  /**
   * Get the stored pss for the whole matrix
   *
   * @return the stored pss
   */
  public List<ParameterServerId> getPss() {
    Set<ParameterServerId> pss = new HashSet<>();
    for (PartitionMeta partMeta : partitionMetas.values()) {
      pss.add(partMeta.getMasterPs());
    }
    return new ArrayList<>(pss);
  }

  /**
   * Set the stored pss for a partition
   *
   * @param partId partition id
   * @param psIds the stored pss
   */
  public void setPss(int partId, List<ParameterServerId> psIds) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return;
    }
    partitionMeta.setPss(psIds);
  }

  /**
   * Get the master stored ps for the partition
   *
   * @param partId partition id
   * @return the master stored ps
   */
  public ParameterServerId getMasterPs(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return null;
    }
    return partitionMeta.getMasterPs();
  }

  /**
   * Get matrix attributes
   *
   * @return matrix attributes
   */
  public Map<String, String> getAttributes() {
    return matrixContext.getAttributes();
  }

  /**
   * Get the block row number for the matrix
   *
   * @return the block row number for the matrix
   */
  public int getBlockRowNum() {
    return matrixContext.getMaxRowNumInBlock();
  }

  /**
   * Get the block column number for the matrix
   *
   * @return the block column number for the matrix
   */
  public long getBlockColNum() {
    return matrixContext.getMaxColNumInBlock();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MatrixContext:").append(matrixContext).append("\n");
    sb.append("partitions:").append("\n");
    List<PartitionMeta> parts = new ArrayList<>(partitionMetas.values());
    parts.sort((PartitionMeta p1, PartitionMeta p2) -> p1.getPartId() - p2.getPartId());
    int size = parts.size();
    sb.append("total partitoin number:" + size).append("\n");
    for (int i = 0; i < size; i++) {
      sb.append("partition ").append(parts.get(i).getPartId()).append(":").append(parts.get(i))
          .append("\n");
    }

    return sb.toString();
  }

  /**
   * Remove the stored ps for all partitions
   *
   * @param psId ps id
   */
  public void removePs(ParameterServerId psId) {
    for (PartitionMeta partMeta : partitionMetas.values()) {
      partMeta.removePs(psId);
    }
  }

  /**
   * Add the stored ps for the partition
   *
   * @param partId partition id
   * @param psId ps id
   */
  public void addPs(int partId, ParameterServerId psId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if (partitionMeta == null) {
      return;
    }
    partitionMeta.addReplicationPS(psId);
  }

  /**
   * Get matrix value type class
   *
   * @return null if this parameter is not set
   * @throws ClassNotFoundException if value class is not found
   */
  public Class<? extends IElement> getValueClass() throws ClassNotFoundException {
    return matrixContext.getValueType();
  }

  /**
   * Get matrix server partition class
   *
   * @return matrix server partition class
   * @throws ClassNotFoundException if server partition class is not found
   */
  public Class<? extends IServerPartition> getPartitionClass() throws ClassNotFoundException {
    return matrixContext.getPartitionClass();
  }

  /**
   * Get matrix server partition storage class
   *
   * @return matrix server partition storage class, null means not set by user
   * @throws ClassNotFoundException if server partition storage class is not found
   */
  public Class<? extends IServerPartitionStorage> getPartitionStorageClass()
      throws ClassNotFoundException {
    return matrixContext.getPartitionStorageClass();
  }

  /**
   * Get PS Matrix initialization function
   *
   * @return PS Matrix initialization function
   */
  public PSMatrixInit getInitFunc() {
    return matrixContext.getInitFunc();
  }

  /**
   * Get total partition number
   *
   * @return total partition number
   */
  public int getPartitionNum() {
    return partitionMetas.size();
  }

  /**
   * Get all partitions that sorted by partition id
   *
   * @return all partitions that sorted by partition id
   */
  public PartitionKey[] getPartitionKeys() {
    return partitionKeys;
  }

  public Class<? extends KeyHash> getRouterHash() {
    return matrixContext.getKeyHasherClass();
  }

  public boolean isHash() {
    return matrixContext.getPartitionerClass() == HashPartitioner.class;
  }

  public int getTotalPartNum() {
    return totalPartNum;
  }

  public int getMatrixId() { return matrixContext.getMatrixId(); }
}
