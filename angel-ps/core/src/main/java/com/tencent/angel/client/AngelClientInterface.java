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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.client;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.model.AlgorithmModel;

/**
 * Angel client interface. It defines application control operations from angel client.
 */
public interface AngelClientInterface {
  /**
   * Add a new matrix.
   * 
   * @param mContext matrix context
   * @throws AngelException
   */
  void addMatrix(MatrixContext mContext) throws AngelException;

  /**
   * Submit application.
   * 
   * @throws AngelException
   */
  void submit() throws AngelException;

  /**
   * Load the model from files.
   * 
   * @param model model
   * @throws AngelException
   */
  void loadModel(AlgorithmModel model) throws AngelException;

  /**
   * Startup workers and start to execute tasks.
   * 
   * @throws AngelException
   */
  void start() throws AngelException;

  /**
   * Wait until all the tasks are done.
   * 
   * @throws AngelException
   */
  void waitForCompletion() throws AngelException;

  /**
   * Write the model to files.
   * 
   * @param model model need to write to files.
   * @throws AngelException
   */
  void saveModel(AlgorithmModel model) throws AngelException;

  /**
   * Stop the whole application.
   * 
   * @throws AngelException stop failed
   */
  void stop() throws AngelException;
}
