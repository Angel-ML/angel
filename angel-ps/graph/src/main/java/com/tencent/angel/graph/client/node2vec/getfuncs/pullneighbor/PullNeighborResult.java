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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PullNeighborResult extends GetResult {
  private Long2ObjectOpenHashMap<long[]> result;

  public PullNeighborResult(Long2ObjectOpenHashMap<long[]> result) {
    super();
    this.result = result;
  }

  public PullNeighborResult(ResponseType responseType, Long2ObjectOpenHashMap<long[]> result) {
    super(responseType);
    this.result = result;
  }

  public PullNeighborResult(ResponseType responseType) {
    super(responseType);
  }

  public PullNeighborResult() {
    super();
  }

  public Long2ObjectOpenHashMap<long[]> getResult() {
    return result;
  }

  public void setResult(Long2ObjectOpenHashMap<long[]> result) {
    this.result = result;
  }
}
