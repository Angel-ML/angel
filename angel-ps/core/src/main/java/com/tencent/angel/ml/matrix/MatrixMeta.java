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

package com.tencent.angel.ml.matrix;

import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ps.ParameterServerId;

import java.util.*;

/**
 * The meta of matrix.
 */
public class MatrixMeta {
  /**
   * Matrix basic parameters
   */
  private final MatrixContext matrixContext;

  /**
   * Matrix partitions parameters
   */
  private final Map<Integer, PartitionMeta> partitionMetas;

  /**
   * Create a MatrixMeta
   * @param mContext matrix context
   */
  public MatrixMeta(MatrixContext mContext) {
    this(mContext, new HashMap<>());
  }

  /**
   * Create a MatrixMeta
   * @param matrixContext matrix context
   * @param partitionMetas matrix partitions meta
   */
  public MatrixMeta(MatrixContext matrixContext, Map<Integer, PartitionMeta> partitionMetas) {
    this.matrixContext = matrixContext;
    this.partitionMetas = partitionMetas;
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
   * @return number of non-zero elements
   */
  public long getValidIndexNum() { return matrixContext.getValidIndexNum(); }

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
   * @param key   the key
   * @param value the default value
   * @return the attribute
   */
  public String getAttribute(String key, String value) {
    if (!matrixContext.getAttributes().containsKey(key))
      return value;
    return matrixContext.getAttributes().get(key);
  }

  /**
   * Gets attribute.
   *
   * @param key   the key
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
    String average =
        getAttribute(MatrixConf.MATRIX_AVERAGE, MatrixConf.DEFAULT_MATRIX_AVERAGE);
    return Boolean.parseBoolean(average);
  }

  /**
   * Is hogwild.
   *
   * @return the result
   */
  public boolean isHogwild() {
    String hogwild =
        getAttribute(MatrixConf.MATRIX_HOGWILD, MatrixConf.DEFAULT_MATRIX_HOGWILD);
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
   * @return all partitions meta
   */
  public Map<Integer, PartitionMeta> getPartitionMetas() {
    return partitionMetas;
  }

  /**
   * Get matrix context
   * @return matrix context
   */
  public MatrixContext getMatrixContext() {
    return matrixContext;
  }

  /**
   * Add meta for a partition
   * @param id partition id
   * @param meta partition meta
   */
  public void addPartitionMeta(int id, PartitionMeta meta) {
    partitionMetas.put(id, meta);
  }

  /**
   * Get meta for a partition
   * @param partId partition id
   * @return partition meta
   */
  public PartitionMeta getPartitionMeta(int partId) {
    return partitionMetas.get(partId);
  }

  /**
   * Get the stored pss for a partition
   * @param partId partition id
   * @return the stored pss
   */
  public List<ParameterServerId> getPss(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if(partitionMeta == null) {
      return null;
    }
    return partitionMeta.getPss();
  }

  /**
   * Set the stored pss for a partition
   * @param partId partition id
   * @param psIds the stored pss
   */
  public void setPss(int partId, List<ParameterServerId> psIds) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if(partitionMeta == null) {
      return;
    }
    partitionMeta.setPss(psIds);
  }

  /**
   * Get the master stored ps for the partition
   * @param partId partition id
   * @return the master stored ps
   */
  public ParameterServerId getMasterPs(int partId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if(partitionMeta == null) {
      return null;
    }
    return partitionMeta.getMasterPs();
  }

  /**
   * Get matrix attributes
   * @return matrix attributes
   */
  public Map<String, String> getAttributes() {
    return matrixContext.getAttributes();
  }

  /**
   * Get the block row number for the matrix
   * @return the block row number for the matrix
   */
  public int getBlockRowNum() {
    return matrixContext.getMaxRowNumInBlock();
  }

  /**
   * Get the block column number for the matrix
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
    parts.sort((PartitionMeta p1, PartitionMeta p2)-> p1.getPartId() - p2.getPartId());
    int size = parts.size();
    sb.append("total partitoin number:" + size).append("\n");
    for(int i = 0; i < size; i++) {
      sb.append("partition ").append(parts.get(i).getPartId()).append(":").append(parts.get(i)).append("\n");
    }

    return sb.toString();
  }

  /**
   * Remove the stored ps for all partitions
   * @param psId ps id
   */
  public void removePs(ParameterServerId psId) {
    for(PartitionMeta partMeta : partitionMetas.values()) {
      partMeta.removePs(psId);
    }
  }

  /**
   * Add the stored ps for the partition
   * @param partId partition id
   * @param psId ps id
   */
  public void addPs(int partId, ParameterServerId psId) {
    PartitionMeta partitionMeta = partitionMetas.get(partId);
    if(partitionMeta == null) {
      return;
    }
    partitionMeta.addReplicationPS(psId);
  }

  /**
   * Get estimate sparsity
   * @return estimate sparsity
   */
  public double getEstSparsity() {
    return matrixContext.getEstSparsity();
  }
}
