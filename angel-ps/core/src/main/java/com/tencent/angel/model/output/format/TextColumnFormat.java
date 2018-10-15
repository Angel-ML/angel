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
 * Text format: column id + sep + row1 element + sep + row2 element + sep + ...
 */
public class TextColumnFormat extends ColumnFormat {
  public String sep = ",";
  @Override public void save(IntFloatsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(IntDoublesCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(IntIntsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(IntLongsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(LongFloatsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(LongDoublesCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(LongIntsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void save(LongLongsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for(int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if(i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override public void load(IntFloatsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Float.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(IntDoublesCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Double.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(IntIntsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Integer.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(IntLongsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Long.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(LongFloatsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Float.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(LongDoublesCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Double.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(LongIntsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Integer.valueOf(subStrs[i+1]);
    }
  }

  @Override public void load(LongLongsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String [] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for(int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Long.valueOf(subStrs[i+1]);
    }
  }
}
