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

package com.tencent.angel.model;

import com.tencent.angel.ml.math2.matrix.Matrix;

import java.util.Map;

/**
 * Model local load result
 */
public class ModelLocalLoadResult {
  /**
   * Matrix name to matrix map
   */
  private final Map<String, Matrix> nameToMatrixMap;

  /**
   * Create new ModelLocalLoadResult
   * @param nameToMatrixMap Matrix name to matrix map
   */
  public ModelLocalLoadResult(Map<String, Matrix> nameToMatrixMap) {
    this.nameToMatrixMap = nameToMatrixMap;
  }

  /**
   * Get all model load result
   * @return matrix name to matrix map
   */
  public Map<String, Matrix> getNameToMatrixMap() {
    return nameToMatrixMap;
  }

  /**
   * Get matrix load result
   * @param name matrix name
   * @return matrix data
   */
  public Matrix getMatrix(String name) {
    return nameToMatrixMap.get(name);
  }
}
