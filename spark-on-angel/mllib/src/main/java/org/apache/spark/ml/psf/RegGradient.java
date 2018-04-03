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

import com.tencent.angel.ml.matrix.psf.update.enhance.zip2.Zip2MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class RegGradient implements Zip2MapWithIndexFunc {

  private double regParam;
  private int interceptIndex;
  private boolean fitIntercept;
  private boolean standardization;
  public RegGradient(
      double regParam, int interceptIndex, boolean fitIntercept, boolean standardization) {
    this.regParam = regParam;
    this.interceptIndex = interceptIndex;
    this.fitIntercept = fitIntercept;
    this.standardization = standardization;
  }

  public RegGradient() {
    super();
  }

  @Override
  public double call(long index, double coeff, double featureStd) {
    boolean isIntercept = index > interceptIndex;
    if (fitIntercept && isIntercept ) {
      return 0.0;
    } else {
      if (standardization || featureStd == 0.0) {
        return coeff * regParam;
      } else {
        return coeff / (featureStd * featureStd) * regParam;
      }
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(regParam);
    buf.writeInt(interceptIndex);
    buf.writeBoolean(fitIntercept);
    buf.writeBoolean(standardization);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    regParam = buf.readDouble();
    interceptIndex = buf.readInt();
    fitIntercept = buf.readBoolean();
    standardization = buf.readBoolean();
  }

  @Override
  public int bufferLen() {
    return 8 + 4 + 1 + 1;
  }
}
