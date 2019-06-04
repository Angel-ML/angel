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

package com.tencent.angel.graph.data;

import java.util.Objects;

public class EdgeId {
  private long fromNodeId;
  private long toNodeId;
  private int type;

  public EdgeId(long fromNodeId, long toNodeId, int type) {
    this.fromNodeId = fromNodeId;
    this.toNodeId = toNodeId;
    this.type = type;
  }

  public long getFromNodeId() {
    return fromNodeId;
  }

  public long getToNodeId() {
    return toNodeId;
  }

  public int getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    EdgeId edgeId = (EdgeId) o;
    return fromNodeId == edgeId.fromNodeId &&
            toNodeId == edgeId.toNodeId &&
            type == edgeId.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromNodeId, toNodeId, type);
  }
}
