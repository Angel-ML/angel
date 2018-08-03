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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;

import io.netty.buffer.ByteBuf;

/**
 * `ScalarPartitionAggrResult` is a result Class for all Aggregate function whose result is double
 */
public class ScalarPartitionAggrResult extends PartitionGetResult {
  public double result;

  public ScalarPartitionAggrResult(double result) {
    this.result = result;
  }

  public ScalarPartitionAggrResult() {}

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(result);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    result = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
