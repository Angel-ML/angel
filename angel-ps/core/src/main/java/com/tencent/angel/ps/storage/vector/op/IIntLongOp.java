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

package com.tencent.angel.ps.storage.vector.op;

import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ps.server.data.request.InitFunc;

/**
 * The operator for the storage that store <int, long> values
 */
public interface IIntLongOp extends IIntKeyOp, ILongValueOp {

  /**
   * Get element value use index
   *
   * @param index element index
   * @return element value
   */
  long get(int index);

  /**
   * Set element value
   *
   * @param index element index
   * @param value new element value
   */
  void set(int index, long value);

  /**
   * Get element values use indices
   *
   * @param indices element indices
   * @return element value
   */
  long[] get(int[] indices);

  /**
   * Set element values
   *
   * @param indices element indices
   * @param values new element values
   */
  void set(int[] indices, long[] values);

  /**
   * Add to the element
   *
   * @param index element index
   * @param value element update
   */
  void addTo(int index, long value);

  /**
   * Add to the elements
   *
   * @param indices element indices
   * @param values element updates
   */
  void addTo(int[] indices, long[] values);

  /**
   * Merge the elements to the given vector
   *
   * @param vector the merge destination vector
   */
  void mergeTo(IntLongVector vector);

  /**
   * Get the element value, if the element does not exist, just init it use the function first
   *
   * @param index element index
   * @param func element initial function
   * @return element value
   */
  long initAndGet(int index, InitFunc func);
}
