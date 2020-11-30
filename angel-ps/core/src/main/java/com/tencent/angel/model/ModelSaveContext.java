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
 * Model save context
 */
public class ModelSaveContext {
  /**
   * Model save directory
   */
  private volatile String savePath;

  /**
   * Temp model save directory
   */
  private volatile String tmpSavePath;

  /**
   * Is checkpoint
   */
  private volatile boolean isCheckpoint = false;

  /**
   * Matrices save contexts
   */
  private final List<MatrixSaveContext> matricesContext;

  /**
   * Create a new ModelSaveContext
   */
  public ModelSaveContext() {
    this(null);
  }

  /**
   * Create a new ModelSaveContext
   *
   * @param finalPath model save directory
   */
  public ModelSaveContext(String finalPath) {
    this(finalPath, new ArrayList<>());
  }

  /**
   * Create a new ModelSaveContext
   *
   * @param savePath        model save directory
   * @param matricesContext matrices save context
   */
  public ModelSaveContext(String savePath, List<MatrixSaveContext> matricesContext) {
    this.savePath = savePath;
    this.matricesContext = matricesContext;
  }

  /**
   * Get matrices save context
   *
   * @return matrices save context
   */
  public List<MatrixSaveContext> getMatricesContext() {
    return matricesContext;
  }

  /**
   * Add a matrix save context
   *
   * @param matrix matrix save context
   */
  public void addMatrix(MatrixSaveContext matrix) {
    matricesContext.add(matrix);
  }

  /**
   * Get model save directory
   *
   * @return model save directory
   */
  public String getSavePath() {
    return savePath;
  }

  /**
   * Get model temporary save directory
   *
   * @return model temporary save directory
   */
  public String getTmpSavePath() {
    return tmpSavePath;
  }

  /**
   * Set temp save directory
   *
   * @param tmpSavePath temp save directory
   */
  public void setTmpSavePath(String tmpSavePath) {
    this.tmpSavePath = tmpSavePath;
  }

  /**
   * Set model save directory
   *
   * @param savePath model save directory
   */
  public void setSavePath(String savePath) {
    this.savePath = savePath;
  }

  /**
   * Is this a checkpoint operation
   * @return true means is a checkpoint operation
   */
  public boolean isCheckpoint() {
    return isCheckpoint;
  }

  /**
   * Set this operation a checkpoint operation
   * @param checkpoint
   */
  public void setCheckpoint(boolean checkpoint) {
    isCheckpoint = checkpoint;
  }
}