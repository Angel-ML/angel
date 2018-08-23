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


package com.tencent.angel.ps.storage.matrix;

import com.tencent.angel.ps.storage.vector.ServerRow;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionSourceMap extends PartitionSource {
  private ConcurrentHashMap<Integer, ServerRow> source;

  public PartitionSourceMap() {
  }

  public void init(int rowNum) {
    source = new ConcurrentHashMap<>(rowNum);
  }

  @Override protected ServerRow getRow(int index) {
    return source.get(index);
  }

  @Override protected void putRow(int index, ServerRow row) {
    source.put(index, row);
  }

  @Override protected int rowNum() {
    return source.size();
  }

  @Override protected boolean hasRow(int index) {
    return source.containsKey(index);
  }

  @Override protected Iterator<Map.Entry<Integer, ServerRow>> iterator() {
    return source.entrySet().iterator();
  }
}
