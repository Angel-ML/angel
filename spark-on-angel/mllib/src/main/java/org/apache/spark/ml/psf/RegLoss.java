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

package org.apache.spark.ml.psf;

import io.netty.buffer.ByteBuf;
import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2MapWithIndexFunc;

public class RegLoss implements Zip2MapWithIndexFunc {

  private int interceptIndex;
  private boolean fitIntercept;
  private boolean standardization;

  public RegLoss(int interceptIndex, boolean fitIntercept, boolean standardization) {
    this.interceptIndex = interceptIndex;
    this.fitIntercept = fitIntercept;
    this.standardization = standardization;
  }

  public RegLoss() {
    super();
  }

  @Override
  public double call(long index, double coefficient, double featStd) {

    boolean isIntercept = (fitIntercept) && (index > interceptIndex);

    double loss = 0.0;
    if (!isIntercept) {
      if (standardization || featStd == 0.0) {
        loss = coefficient * coefficient;
      } else {
        loss = coefficient * coefficient / (featStd * featStd);
      }
    }
    return loss;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(interceptIndex);
    buf.writeBoolean(fitIntercept);
    buf.writeBoolean(standardization);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    interceptIndex = buf.readInt();
    fitIntercept = buf.readBoolean();
    standardization = buf.readBoolean();
  }

  @Override
  public int bufferLen() {
    return 4 + 1 + 1;
  }
}
