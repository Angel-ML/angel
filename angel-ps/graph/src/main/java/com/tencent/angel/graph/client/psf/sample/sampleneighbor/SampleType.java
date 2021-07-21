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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import java.util.HashMap;
import java.util.Map;


/**
 * Sample type,such as : SIMPLE, NODE, EDGE, NODE_AND_EDGE;
 * SIMPLE: sample neighbors randomly by number count.
 * NODE: sample neighbors randomly with neighbor type
 * EDGE: sample neighbors randomly with edge type by number count
 * NODE_AND_EDGE: sample neighbors randomly with neighbor type and edge type by number count
 */
public enum SampleType {
  SIMPLE(0),
  NODE(1),
  EDGE(2),
  NODE_AND_EDGE(3);

  static final Map<Integer, SampleType> indexToOpMap = new HashMap<>();

  static {
    indexToOpMap.put(SIMPLE.index, SIMPLE);
    indexToOpMap.put(NODE.index, NODE);
    indexToOpMap.put(EDGE.index, EDGE);
    indexToOpMap.put(NODE_AND_EDGE.index, NODE_AND_EDGE);
  }

  public static SampleType valueOf(int index) {
    return indexToOpMap.get(index);
  }

  private final int index;

  SampleType(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }
}
