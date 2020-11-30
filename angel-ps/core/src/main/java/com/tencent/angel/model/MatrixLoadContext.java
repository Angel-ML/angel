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

/**
 * Matrix load context
 */
public class MatrixLoadContext {
  /**
   * Matrix name
   */
  private final String matrixName;

  /**
   * Matrix load path
   */
  private String loadPath;

  /**
   * Create a new MatrixLoadContext
   *
   * @param matrixName matrix name
   */
  public MatrixLoadContext(String matrixName, String loadPath) {
    this.matrixName = matrixName;
    this.loadPath = loadPath;
  }

  /**
   * Create a new MatrixLoadContext
   *
   * @param matrixName matrix name
   */
  public MatrixLoadContext(String matrixName) {
    this(matrixName, null);
  }

  /**
   * Get matrix name
   *
   * @return matrix name
   */
  public String getMatrixName() {
    return matrixName;
  }

  /**
   * Get matrix load path
   * @return matrix load path
   */
  public String getLoadPath() {
    return loadPath;
  }

  /**
   * Set load path
   * @param loadPath load path
   */
  public void setLoadPath(String loadPath) {
    this.loadPath = loadPath;
  }
}
