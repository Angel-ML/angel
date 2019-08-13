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

import com.tencent.angel.common.DeepClone;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.common.StreamSerialize;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import io.netty.buffer.ByteBuf;

/**
 * General ps-row storage operations
 */
public interface GeneralOp extends DeepClone, Serialize, StreamSerialize {

  /**
   * Clear the storage
   */
  void clear();

  /**
   * Get the element number in the storage
   *
   * @return the element number in the storage
   */
  int size();

  /**
   * Is the dense storage
   *
   * @return true means dense storage
   */
  boolean isDense();

  /**
   * Is the sparse storage
   *
   * @return true means sparse storage
   */
  boolean isSparse();

  /**
   * Is the sorted storage
   *
   * @return true means sorted storage
   */
  boolean isSorted();

  /**
   * Use different clone method as storage method
   * @return clone object
   */
  Object adaptiveClone();

  /**
   * Pipeline update interface
   *
   * @param updateType update data storage method
   * @param buf un-deserialized update data
   * @param op update method
   */
  void update(RowType updateType, ByteBuf buf, UpdateOp op);
}
