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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

/**
 * The interface for Row updater.
 */
public interface RowUpdaterInterface {
  /**
   * Update.
   *
   * @param updateRowType the update row type
   * @param size          the size of updating
   * @param dataBuf       the data buf
   * @param row           the row of updating
   * @throws Exception
   */
  void update(RowType updateRowType, ByteBuf dataBuf, ServerRow row)
      throws Exception;

}
