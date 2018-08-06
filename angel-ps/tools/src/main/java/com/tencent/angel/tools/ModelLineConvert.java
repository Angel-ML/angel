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

package com.tencent.angel.tools;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * Model convert interface
 */
public interface ModelLineConvert {
  /**
   * Serialize row index
   * @param output output stream
   * @param rowIndex row index
   * @throws IOException
   */
  void convertRowIndex(FSDataOutputStream output, int rowIndex) throws IOException;

  /**
   * Serialize a double element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertDouble(FSDataOutputStream output, int index, double value) throws IOException;

  /**
   * Serialize a float element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertFloat(FSDataOutputStream output, int index, float value) throws IOException;

  /**
   * Serialize a int element
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertInt(FSDataOutputStream output, int index, float value) throws IOException;

  /**
   * Serialize a double element with longkey
   * @param output output stream
   * @param index element index
   * @param value element value
   * @throws IOException
   */
  void convertDoubleLongKey(FSDataOutputStream output, long index, double value) throws IOException;
}
