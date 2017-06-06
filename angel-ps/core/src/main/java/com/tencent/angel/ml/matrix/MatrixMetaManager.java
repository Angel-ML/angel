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

import com.tencent.angel.psagent.PSAgentContext;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The matrix meta manager.
 */
public class MatrixMetaManager {
  private final ConcurrentHashMap<Integer, MatrixMeta> matrixMetas;
  private final ConcurrentHashMap<String, Integer> matrixNameIndex;

  /**
   * Creates a new matrix meta manager.
   */
  public MatrixMetaManager() {
    this.matrixMetas = new ConcurrentHashMap<Integer, MatrixMeta>();
    this.matrixNameIndex = new ConcurrentHashMap<String, Integer>();
  }

  /**
   * Add matrixes.
   *
   * @param matrixMetas the matrix metas
   */
  public void addMatrixes(Map<Integer, MatrixMeta> matrixMetas) {
    for (Entry<Integer, MatrixMeta> matrixMetaEntry : matrixMetas.entrySet()) {
      this.matrixMetas.putIfAbsent(matrixMetaEntry.getKey(), matrixMetaEntry.getValue());
      this.matrixNameIndex.putIfAbsent(matrixMetaEntry.getValue().getName(),
          matrixMetaEntry.getKey());
    }
  }

  /**
   * Add matrix.
   *
   * @param matrixMeta the matrix meta
   */
  public void addMatrix(MatrixMeta matrixMeta) {
    this.matrixMetas.putIfAbsent(matrixMeta.getId(), matrixMeta);
    this.matrixNameIndex.putIfAbsent(matrixMeta.getName(), matrixMeta.getId());
  }

  /**
   * Gets matrix id.
   *
   * @param matrixName the matrix name
   * @return the matrix id
   */
  public int getMatrixId(String matrixName) {
    if (matrixNameIndex.containsKey(matrixName)) {
      return matrixNameIndex.get(matrixName);
    } else {
      synchronized(this) {
        if (!matrixNameIndex.containsKey(matrixName)) {
          try {
            PSAgentContext.get().getPsAgent().refreshMatrixInfo();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        if (matrixNameIndex.containsKey(matrixName)) {
          return matrixNameIndex.get(matrixName);
        } else {
          return -1;
        }
      }
    }
  }

  /**
   * Gets attribute.
   *
   * @param matrixId the matrix id
   * @param key      the key
   * @param value    the value
   * @return the attribute
   */
  public String getAttribute(int matrixId, String key, String value) {
    if (!exist(matrixId)) {
      return null;
    } else {
      return matrixMetas.get(matrixId).getAttribute(key, value);
    }
  }
  
  /**
   * Gets attribute.
   *
   * @param matrixId the matrix id
   * @param key      the key
   * @return the attribute
   */
  public String getAttribute(int matrixId, String key) {
    if (!exist(matrixId)) {
      return null;
    } else {
      return matrixMetas.get(matrixId).getAttribute(key);
    }
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixId the matrix id
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(int matrixId) {
    if (!exist(matrixId)) {
      return null;
    } else {
      return matrixMetas.get(matrixId);
    }
  }

  /**
   * Gets matrix meta.
   *
   * @param matrixName the matrix name
   * @return the matrix meta
   */
  public MatrixMeta getMatrixMeta(String matrixName) {
    return matrixMetas.get(getMatrixId(matrixName));
  }

  /**
   * Gets matrix ids.
   *
   * @return the matrix ids
   */
  public Set<Integer> getMatrixIds() {
    return matrixMetas.keySet();
  }

  /**
   * Gets matrix names.
   *
   * @return the matrix names
   */
  public Set<String> getMatrixNames() {
    return matrixNameIndex.keySet();
  }

  /**
   * Exist boolean.
   *
   * @param matrixId the matrix id
   * @return the result
   */
  public boolean exist(int matrixId) {
    if (!matrixMetas.containsKey(matrixId)) {
      synchronized(this) {
        if (!matrixMetas.containsKey(matrixId)) {
          try {
            PSAgentContext.get().getPsAgent().refreshMatrixInfo();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    return matrixMetas.containsKey(matrixId);
  }
}
