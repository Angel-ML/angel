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
import io.netty.buffer.ByteBuf;

public class FTRLWUpdater implements Zip2MapFunc {

  private Double alpha;
  private Double beta;
  private Double lambda1;
  private Double lambda2;

  public FTRLWUpdater() {
    super();
  }

  public FTRLWUpdater(Double alpha, Double beta, Double lambda1, Double lambda2) {

    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
  }

  @Override
  public double call(double zVal, double nVal) {
    if (Math.abs(zVal) > lambda1){
      return (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal - Math.signum(zVal) * lambda1);
    }else{
      return 0.0;
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(alpha);
    buf.writeDouble(beta);
    buf.writeDouble(lambda1);
    buf.writeDouble(lambda2);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    alpha = buf.readDouble();
    beta = buf.readDouble();
    lambda1 = buf.readDouble();
    lambda2 = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 4 * 8;
  }

}