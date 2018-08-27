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

import java.util.ArrayList;
import java.util.List;

/**
 * Model load context
 */
public class ModelLoadContext {
  /**
   * Model save directory, load model from it
   */
  private final String loadPath;

  /**
   * Matrices load context
   */
  private final List<MatrixLoadContext> matricesContext;

  /**
   * Create a new ModelLoadContext
   *
   * @param loadPath        Model save directory
   * @param matricesContext matrices load context
   */
  public ModelLoadContext(String loadPath, List<MatrixLoadContext> matricesContext) {
    this.loadPath = loadPath;
    this.matricesContext = matricesContext;
  }

  /**
   * Create a new ModelLoadContext
   *
   * @param loadPath Model save directory
   */
  public ModelLoadContext(String loadPath) {
    this(loadPath, new ArrayList<>());
  }

  /**
   * Create a new ModelLoadContext
   */
  public ModelLoadContext() {
    this(null, new ArrayList<>());
  }

  /**
   * Add matrix load context
   *
   * @param matrixContext matrix load context
   */
  public void addMatrix(MatrixLoadContext matrixContext) {
    matricesContext.add(matrixContext);
  }

  /**
   * Get model save path
   *
   * @return model save path
   */
  public String getLoadPath() {
    return loadPath;
  }

  /**
   * Get model load contexts
   *
   * @return model load contexts
   */
  public List<MatrixLoadContext> getMatricesContext() {
    return matricesContext;
  }
}