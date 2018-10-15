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

package com.tencent.angel.model.output.format;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Binary format: column id, column values
 */
public class BinaryColumnFormat extends ColumnFormat {
  @Override public void save(IntFloatsCol col, DataOutputStream output) throws IOException {
    output.writeInt(col.colId);
    for(float value : col.colElems) {
      output.writeFloat(value);
    }
  }

  @Override public void save(IntDoublesCol col, DataOutputStream output) throws IOException {
    output.writeInt(col.colId);
    for(double value : col.colElems) {
      output.writeDouble(value);
    }
  }

  @Override public void save(IntIntsCol col, DataOutputStream output) throws IOException {
    output.writeInt(col.colId);
    for(int value : col.colElems) {
      output.writeInt(value);
    }
  }

  @Override public void save(IntLongsCol col, DataOutputStream output) throws IOException {
    output.writeInt(col.colId);
    for(long value : col.colElems) {
      output.writeLong(value);
    }
  }

  @Override public void save(LongFloatsCol col, DataOutputStream output) throws IOException {
    output.writeLong(col.colId);
    for(float value : col.colElems) {
      output.writeFloat(value);
    }
  }

  @Override public void save(LongDoublesCol col, DataOutputStream output) throws IOException {
    output.writeLong(col.colId);
    for(double value : col.colElems) {
      output.writeDouble(value);
    }
  }

  @Override public void save(LongIntsCol col, DataOutputStream output) throws IOException {
    output.writeLong(col.colId);
    for(int value : col.colElems) {
      output.writeInt(value);
    }
  }

  @Override public void save(LongLongsCol col, DataOutputStream output) throws IOException {
    output.writeLong(col.colId);
    for(long value : col.colElems) {
      output.writeLong(value);
    }
  }

  @Override public void load(IntFloatsCol col, DataInputStream input) throws IOException {
    col.colId = input.readInt();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readFloat();
    }
  }

  @Override public void load(IntDoublesCol col, DataInputStream input) throws IOException {
    col.colId = input.readInt();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readDouble();
    }
  }

  @Override public void load(IntIntsCol col, DataInputStream input) throws IOException {
    col.colId = input.readInt();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readInt();
    }
  }

  @Override public void load(IntLongsCol col, DataInputStream input) throws IOException {
    col.colId = input.readInt();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readLong();
    }
  }

  @Override public void load(LongFloatsCol col, DataInputStream input) throws IOException {
    col.colId = input.readLong();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readFloat();
    }
  }

  @Override public void load(LongDoublesCol col, DataInputStream input) throws IOException {
    col.colId = input.readLong();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readDouble();
    }
  }

  @Override public void load(LongIntsCol col, DataInputStream input) throws IOException {
    col.colId = input.readLong();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readInt();
    }
  }

  @Override public void load(LongLongsCol col, DataInputStream input) throws IOException {
    col.colId = input.readLong();
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = input.readLong();
    }
  }
}
