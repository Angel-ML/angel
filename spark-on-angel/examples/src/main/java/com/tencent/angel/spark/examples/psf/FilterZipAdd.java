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

package com.tencent.angel.spark.examples.psf;

import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class FilterZipAdd implements Zip2MapWithIndexFunc {

  private int zeroIndex;

  public FilterZipAdd(int zeroIndex) {
    this.zeroIndex = zeroIndex;
  }

  public FilterZipAdd() {}

  @Override
  public double call(long index, double value1, double value2) {
    double result = 0;
    if (index != zeroIndex) {
      result = value1 + value2;
    }
    return result;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(zeroIndex);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    zeroIndex = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return 4;
  }

}
