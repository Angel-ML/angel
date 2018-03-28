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

import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2MapFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.zip3.Zip3MapFunc;
import io.netty.buffer.ByteBuf;

public class Adjust implements Zip2MapFunc, Zip3MapFunc {
  private double l1reg = 0.0;

  public Adjust() {}

  public Adjust(double l1reg) {
    this.l1reg = l1reg;
  }

  // implement Zip2Map interface
  @Override
  public double call(double xv, double grad) {
    return call(xv, grad, l1reg);
  }

  // implement Zip3Map interface
  @Override
  public double call(double xv, double grad, double l1regValue) {
    if (l1regValue < 0) {
      throw new IllegalArgumentException("negative l1reg value!");
    }

    if (l1regValue == 0.0) {
      return grad;
    } else if (xv == 0.0) {
      double deltaPlus = grad + l1regValue;
      double deltaMinus = grad - l1regValue;
      if (deltaMinus > 0) {
        return deltaMinus;
      } else if (deltaPlus < 0) {
        return deltaPlus;
      } else {
        return 0.0;
      }
    } else {
      return grad + Math.signum(xv) * l1regValue;
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(l1reg);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.l1reg = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

}
