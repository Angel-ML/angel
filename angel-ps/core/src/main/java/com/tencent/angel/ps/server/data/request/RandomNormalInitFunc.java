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

package com.tencent.angel.ps.server.data.request;

import io.netty.buffer.ByteBuf;

import java.util.Random;

public class RandomNormalInitFunc implements InitFunc {
  private double mean;
  private double stdDev;
  private Random rand = new Random(System.currentTimeMillis());

  public RandomNormalInitFunc(double mean, double stdDev) {
    this.mean = mean;
    this.stdDev = stdDev;
  }

  public RandomNormalInitFunc() {
    this(0.0, 0.0);
  }

  @Override
  public double action() {
    return stdDev * rand.nextGaussian() + mean;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeDouble(mean);
    buf.writeDouble(stdDev);
  }

  @Override public void deserialize(ByteBuf buf) {
    this.mean = buf.readDouble();
    this.stdDev = buf.readDouble();
  }

  @Override public int bufferLen() {
    return 16;
  }
}
