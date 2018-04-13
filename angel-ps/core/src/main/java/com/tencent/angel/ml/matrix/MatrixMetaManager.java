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

import com.tencent.angel.ps.ParameterServerId;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The matrix meta manager.
 */
public class MatrixMetaManager {
  /**
   * Matrix id to matrix meta map
   */
  private final ConcurrentHashMap<Integer, MatrixMeta> matrixIdToMetaMap;

  /**
   * Matrix name to matrix id map
   */
  private final ConcurrentHashMap<String, Integer> matrixNameToIdMap;

  /**
   * Creates a new matrix meta manager.
   */
  public MatrixMetaManager() {
    this.matrixIdToMetaMap = new ConcurrentHashMap<>();
    this.matrixNameToIdMap = new ConcurrentHashMap<>();
  }

  /**
   * Add matrixes.
   *
   * @param matrixMetas the matrix metas
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) {
    int size = matrixMetas.size();
    for(int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /**
   * Add matrix.
   *
   * @param matrixMeta the matrix meta
   */
  public void addMatrix(MatrixMeta matrixMeta) {
    this.matrixIdToMetaMap.putIfAbsent(matrixMeta.getId(), matrixMeta);
    this.matrixNameToIdMap.putIfAbsent(matrixMeta.getName(), matrixMeta.getId());
  }

  /**
   * Remove a matrix
   * @param matrixId matrix id
   */
  public void removeMatrix(int matrixId) {
    MatrixMeta meta = matrixIdToMetaMap.remove(matrixId);
    if(meta != null) {
      matrixNameToIdMap.remove(meta.getName());
    }
  }

  /**
   * Gets matrix id.
   *
   * @param matrixName the matrix name
   * @return the matrix id
   */
  public int getMatrixId(String matrixName) {
    if (matrixNameToIdMap.containsKey(matrixName)) {
      return matrixNameToIdMap.get(matrixName);
    } else {
      return -1;
    }
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixId the matrix id
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(int matrixId) {
    return matrixIdToMetaMap.get(matrixId);
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixName the matrix name
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(String matrixName) {
    int matrixId = getMatrixId(matrixName);
    if(matrixId == -1) {
      return null;
    } else {
      return matrixIdToMetaMap.get(matrixId);
    }
  }

  /**
   * Gets matrix ids.
   *
   * @return the matrix ids
   */
  public Set<Integer> getMatrixIds() {
    return matrixIdToMetaMap.keySet();
  }

  /**
   * Gets matrix names.
   *
   * @return the matrix names
   */
  public Set<String> getMatrixNames() {
    return matrixNameToIdMap.keySet();
  }

  /**
   * Is a matrix exist
   * @param matrixName matrix name
   * @return true means exist
   */
  public boolean exists(String matrixName) {
    return matrixNameToIdMap.containsKey(matrixName);
  }

  /**
   * Is a matrix exist
   * @param matrixId matrix id
   * @return true means exist
   */
  public boolean exists(int matrixId) { return matrixIdToMetaMap.containsKey(matrixId); }

  /**
   * Get all matrices meta
   * @return all matrices meta
   */
  public Map<Integer,MatrixMeta> getMatrixMetas() {
    return matrixIdToMetaMap;
  }

  /**
   * Get the stored pss for a matrix partition
   * @param matrixId matrix id
   * @param partId partition id
   * @return the stored pss
   */
  public List<ParameterServerId> getPss(int matrixId, int partId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if(matrixMeta == null) {
      return null;
    }

    return matrixMeta.getPss(partId);
  }

  /**
   * Remove the matrix
   * @param matrixName matrix name
   */
  public void removeMatrix(String matrixName) {
    if(matrixNameToIdMap.containsKey(matrixName)) {
      int matrixId = matrixNameToIdMap.remove(matrixName);
      matrixIdToMetaMap.remove(matrixId);
    }
  }

  /**
   * Remove all matrices
   */
  public void clear() {
    matrixIdToMetaMap.clear();
    matrixNameToIdMap.clear();
  }

  /**
   * Get the master stored ps for a matrix partition
   * @param matrixId matrix id
   * @param partId partition id
   * @return the master stored ps
   */
  public ParameterServerId getMasterPs(int matrixId, int partId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if(matrixMeta == null) {
      return null;
    }

    return matrixMeta.getMasterPs(partId);
  }

  /**
   * Remove matrices
   * @param matrixIds matrix ids
   */
  public void removeMatrices(List<Integer> matrixIds) {
    int size = matrixIds.size();
    for(int i = 0; i < size; i++) {
      removeMatrix(matrixIds.get(i));
    }
  }

  /**
   * Remove the stored ps for all matrix partitions
   * @param psId ps id
   */
  public void removePs(ParameterServerId psId) {
    for(MatrixMeta matrixMeta : matrixIdToMetaMap.values()) {
      matrixMeta.removePs(psId);
    }
  }

  /**
   * Set the stored pss for a matrix partition
   * @param matrixId matrix id
   * @param partId partition id
   * @param psIds the stored pss
   */
  public void setPss(int matrixId, int partId, List<ParameterServerId> psIds) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if(matrixMeta == null) {
      return;
    }

    matrixMeta.setPss(partId, psIds);
  }

  /**
   * Add a the stored ps for a matrix partition
   * @param matrixId matrix id
   * @param partId
   * @param psId ps id
   */
  public void addPs(int matrixId, int partId, ParameterServerId psId) {
    MatrixMeta matrixMeta = matrixIdToMetaMap.get(matrixId);
    if(matrixMeta == null) {
      return;
    }

    matrixMeta.addPs(partId, psId);
  }

  /**
   * Get estimate sparsity
   * @param matrixId matrix id
   * @return estimate sparsity
   */
  public double getEstSparsity(int matrixId) {
    MatrixMeta meta = matrixIdToMetaMap.get(matrixId);
    if(meta == null) {
      return 1.0;
    } else {
      return meta.getEstSparsity();
    }
  }
}
