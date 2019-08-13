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

package com.tencent.angel.ps.storage.partition.storage;

import io.netty.buffer.ByteBuf;
import java.util.List;

/**
 * CSR storage with int values
 */
public class IntCSRStorage extends CSRStorage {
  int [] values;

  private List<int[]> tempRowIds;
  private List<int[]> tempRowLens;
  private List<int[]> tempColumnIndices;

  public IntCSRStorage(int rowIdOffset) {
    super(rowIdOffset);
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);

    // Serialize values
    output.writeInt(values.length);
    for(int i = 0; i < values.length; i++) {
      output.writeInt(values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);

    // Deserialize values
    int size = input.readInt();
    values = new int[size];
    for(int i = 0; i < size; i++) {
      values[i] = input.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + values.length * 4;
  }

  public int[] getValues() {
    return values;
  }

  public void setValues(int[] values) {
    this.values = values;
  }

  public List<int[]> getTempRowIds() {
    return tempRowIds;
  }

  public void setTempRowIds(List<int[]> tempRowIds) {
    this.tempRowIds = tempRowIds;
  }

  public List<int[]> getTempRowLens() {
    return tempRowLens;
  }

  public void setTempRowLens(List<int[]> tempRowLens) {
    this.tempRowLens = tempRowLens;
  }

  public List<int[]> getTempColumnIndices() {
    return tempColumnIndices;
  }

  public void setTempColumnIndices(List<int[]> tempColumnIndices) {
    this.tempColumnIndices = tempColumnIndices;
  }
}
