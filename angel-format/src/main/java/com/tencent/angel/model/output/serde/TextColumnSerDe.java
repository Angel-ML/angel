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

package com.tencent.angel.model.output.serde;

import com.tencent.angel.model.output.element.IntDoublesCol;
import com.tencent.angel.model.output.element.IntFloatsCol;
import com.tencent.angel.model.output.element.IntIntsCol;
import com.tencent.angel.model.output.element.IntLongsCol;
import com.tencent.angel.model.output.element.LongDoublesCol;
import com.tencent.angel.model.output.element.LongFloatsCol;
import com.tencent.angel.model.output.element.LongIntsCol;
import com.tencent.angel.model.output.element.LongLongsCol;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Text format: column id + sep + row1 element + sep + row2 element + sep + ...
 */
public class TextColumnSerDe extends ColumnSerDeImpl {

  private final String defaultSet = ",";
  public final static String sepParam = "text.format.filed.sep";
  private final String sep;

  public TextColumnSerDe(Configuration conf) {
    super(conf);
    sep = conf.get(sepParam, defaultSet);
  }

  @Override
  public void serialize(IntFloatsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(IntDoublesCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(IntIntsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(IntLongsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(LongFloatsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(LongDoublesCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(LongIntsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void serialize(LongLongsCol col, DataOutputStream output) throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append(col.colId);
    sb.append(sep);
    for (int i = 0; i < col.colElems.length; i++) {
      sb.append(col.colElems[i]);
      if (i < col.colElems.length - 1) {
        sb.append(sep);
      }
    }
    sb.append("\n");
    output.writeBytes(sb.toString());
  }

  @Override
  public void deserialize(IntFloatsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Float.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(IntDoublesCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Double.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(IntIntsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Integer.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(IntLongsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Integer.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Long.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(LongFloatsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Float.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(LongDoublesCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Double.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(LongIntsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Integer.valueOf(subStrs[i + 1]);
    }
  }

  @Override
  public void deserialize(LongLongsCol col, DataInputStream input) throws IOException {
    String line = input.readLine();
    String[] subStrs = line.split(sep);
    col.colId = Long.valueOf(subStrs[0]);
    for (int i = 0; i < col.colElems.length; i++) {
      col.colElems[i] = Long.valueOf(subStrs[i + 1]);
    }
  }
}
