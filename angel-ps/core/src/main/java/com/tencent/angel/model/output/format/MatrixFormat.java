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

package com.tencent.angel.model.output.format;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Model format interface
 */
public interface MatrixFormat {
  /**
   * Write the ServerMatrix to files
   * @param matrix the ServerMatrix
   * @param saveContext save context
   * @throws IOException
   */
  void save(ServerMatrix matrix, PSMatrixSaveContext saveContext, Configuration conf) throws IOException;

  /**
   * Load the ServerMatrix from files
   * @param matrix the ServerMatrix
   * @param loadContext load context
   * @throws IOException
   */
  void load(ServerMatrix matrix, PSMatrixLoadContext loadContext, Configuration conf) throws IOException;

  /**
   * Write the Matrix to files
   * @param matrix the ServerMatrix
   * @param saveContext save context
   * @throws IOException
   */
  void save(Matrix matrix, MatrixSaveContext saveContext, Configuration conf) throws IOException;

  /**
   * Load the Matrix from files
   * @param matrix the ServerMatrix
   * @param loadContext load context
   * @throws IOException
   */
  void load(Matrix matrix, MatrixLoadContext loadContext, Configuration conf) throws IOException;
}
