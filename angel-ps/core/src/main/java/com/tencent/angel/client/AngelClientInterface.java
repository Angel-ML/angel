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
import com.tencent.angel.ml.model.MLModel;
import com.tencent.angel.worker.task.BaseTask;

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
  void startPSServer() throws AngelException;

  /**
   * Load the model from files.
   * 
   * @param model model
   * @throws AngelException
   */
  void loadModel(MLModel model) throws AngelException;

  /**
   * Accept specified task and start
   *
   * @param taskClass
   * @throws AngelException
   */
  void runTask(@SuppressWarnings("rawtypes") Class<? extends BaseTask> taskClass) throws AngelException;

  /**
   * Startup workers and start to execute tasks.
   *
   * Use #runTask instead
   *
   * @throws AngelException
   */
  @Deprecated
  void run() throws AngelException;

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
  void saveModel(MLModel model) throws AngelException;

  /**
   * Stop the whole application.
   * 
   * @throws AngelException stop failed
   */
  void stop() throws AngelException;

  /**
   * Stop the whole application with given state.
   *
   * @param stateCode 0:succeed,1:killed,2:failed
   * @throws AngelException stop failed
   */
  void stop(int stateCode) throws AngelException;
}
