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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

public class PartitionSourceArray implements PartitionSource {

  private ServerRow[] source;

  public PartitionSourceArray() {
  }

  public void init(int rowNum) {
    source = new ServerRow[rowNum];
  }

  @Override public ServerRow getRow(int index) {
    return source[index];
  }

  @Override public void putRow(int index, ServerRow row) {
    source[index] = row;
  }

  @Override public int rowNum() {
    return source.length;
  }

  @Override public boolean hasRow(int index) {
    return index >= 0 && index < source.length;
  }

  @Override public Iterator<Map.Entry<Integer, ServerRow>> iterator() {


    return new Iterator<Map.Entry<Integer, ServerRow>>() {
      int currIndex = 0;

      @Override public boolean hasNext() {
        return ++currIndex < source.length;
      }

      @Override public Map.Entry<Integer, ServerRow> next() {
        return new AbstractMap.SimpleEntry(currIndex, source[currIndex]);
      }
    };

  }
}
