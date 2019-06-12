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

package com.tencent.angel.model.io;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Matrix loader/saver interface
 */
public interface MatrixLoaderSaver {

  /**
   * Write the Matrix to files
   *
   * @param matrix the ServerMatrix
   * @param saveContext save context
   * @param conf system configuration
   */
  void save(Matrix matrix, MatrixSaveContext saveContext, Configuration conf) throws IOException;

  /**
   * Load the Matrix from files
   *
   * @param loadContext load context
   * @param conf system configuration
   * @return Matrix loaded matrix
   */
  Matrix load(MatrixLoadContext loadContext, Configuration conf) throws IOException;
}
