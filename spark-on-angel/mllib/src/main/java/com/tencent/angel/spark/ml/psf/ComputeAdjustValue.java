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
 *
 */

package com.tencent.angel.spark.ml.psf;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class ComputeAdjustValue implements MapFunc, Zip2MapFunc {

  private double l1reg = 0.0;

  public ComputeAdjustValue() {}

  public ComputeAdjustValue(double l1reg) {
    this.l1reg = l1reg;
  }

  @Override
  public double call(double xv) {
    return call(xv, l1reg);
  }

  @Override
  public double call(double xv, double l1regValue) {
    if (l1regValue < 0) {
      throw new IllegalArgumentException("negative l1reg value!");
    }

    if (l1regValue == 0.0) {
      return 0.0;
    } else {
      return Math.abs(l1regValue * xv);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(l1reg);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    l1reg = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

}
