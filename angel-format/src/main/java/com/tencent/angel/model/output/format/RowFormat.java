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

import com.tencent.angel.ml.servingmath2.vector.IntDoubleVector;
import com.tencent.angel.ml.servingmath2.vector.IntFloatVector;
import com.tencent.angel.ml.servingmath2.vector.IntIntVector;
import com.tencent.angel.ml.servingmath2.vector.IntLongVector;
import com.tencent.angel.ml.servingmath2.vector.LongDoubleVector;
import com.tencent.angel.ml.servingmath2.vector.LongFloatVector;
import com.tencent.angel.ml.servingmath2.vector.LongIntVector;
import com.tencent.angel.ml.servingmath2.vector.LongLongVector;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Base format for row-base matrix format
 */
public interface RowFormat extends Format {

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(IntFloatVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(IntDoubleVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(IntIntVector row, DataOutputStream output, RowFormatContext context) throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(IntLongVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(LongFloatVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(LongDoubleVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(LongIntVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;

  /**
   * Write a matrix row to output stream
   *
   * @param row matrix row
   * @param output output stream
   * @param context format context
   */
  void save(LongLongVector row, DataOutputStream output, RowFormatContext context)
      throws IOException;


  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(IntFloatVector row, DataInputStream input, RowFormatContext context) throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(IntDoubleVector row, DataInputStream input, RowFormatContext context)
      throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(IntIntVector row, DataInputStream input, RowFormatContext context) throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(IntLongVector row, DataInputStream input, RowFormatContext context) throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(LongFloatVector row, DataInputStream input, RowFormatContext context)
      throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(LongDoubleVector row, DataInputStream input, RowFormatContext context)
      throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(LongIntVector row, DataInputStream input, RowFormatContext context) throws IOException;

  /**
   * Read a matrix row from input stream
   *
   * @param row matrix row
   * @param input input stream
   * @param context format context
   */
  void load(LongLongVector row, DataInputStream input, RowFormatContext context) throws IOException;
}
