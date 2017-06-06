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

package com.tencent.angel.spark.ml.pof;

import com.tencent.angel.ml.matrix.psf.updater.zip3.Zip3MapFunc;
import io.netty.buffer.ByteBuf;

public class Adjust implements Zip3MapFunc {

  public Adjust() {}

  @Override
  public double call(double xv, double v, double l1regValue) {
    if (l1regValue < 0) {
      throw new IllegalArgumentException("negative l1reg value!");
    }

    if (l1regValue == 0.0) {
      return v;
    } else if (xv == 0.0) {
      double deltaPlus = v + l1regValue;
      double deltaMinus = v - l1regValue;
      if (deltaMinus > 0) {
        return deltaMinus;
      } else if (deltaPlus < 0) {
        return deltaPlus;
      } else {
        return 0.0;
      }
    } else {
      return v + Math.signum(xv) * l1regValue;
    }
  }

  @Override
  public void serialize(ByteBuf buf) {}

  @Override
  public void deserialize(ByteBuf buf) {}

  @Override
  public int bufferLen() {
    return 0;
  }

}
