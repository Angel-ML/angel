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

import java.text.DecimalFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Text format: row id + sep + column id + sep + elememt value
 */
public class RowIdColIdValueTextRowFormat extends RowFormat {
  private final String defaultSet = ",";
  public final static String sepParam =  "text.format.filed.sep";
  private final String sep;
  private final DecimalFormat format = new DecimalFormat("#.######");

  public RowIdColIdValueTextRowFormat(Configuration conf) {
    super(conf);
    sep = conf.get(sepParam, defaultSet);
  }

  @Override public void save(IntFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void save(IntDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(format.format(element.value)) + "\n");
  }

  @Override public void save(IntIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void save(IntLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void save(LongFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void save(LongDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(format.format(element.value)) + "\n");
  }

  @Override public void save(LongIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void save(LongLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(
      String.valueOf(element.rowId) + sep + String.valueOf(element.colId) + sep + String
        .valueOf(element.value) + "\n");
  }

  @Override public void load(IntFloatElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Integer.valueOf(kv[1]);
    element.value = Float.valueOf(kv[2]);
  }

  @Override public void load(IntDoubleElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Integer.valueOf(kv[1]);
    element.value = Double.valueOf(kv[2]);
  }

  @Override public void load(IntIntElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Integer.valueOf(kv[1]);
    element.value = Integer.valueOf(kv[2]);
  }

  @Override public void load(IntLongElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Integer.valueOf(kv[1]);
    element.value = Long.valueOf(kv[2]);
  }

  @Override public void load(LongFloatElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Long.valueOf(kv[1]);
    element.value = Float.valueOf(kv[2]);
  }

  @Override public void load(LongDoubleElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Long.valueOf(kv[1]);
    element.value = Double.valueOf(kv[2]);
  }

  @Override public void load(LongIntElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Long.valueOf(kv[1]);
    element.value = Integer.valueOf(kv[2]);
  }

  @Override public void load(LongLongElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.rowId = Integer.valueOf(kv[0]);
    element.colId = Long.valueOf(kv[1]);
    element.value = Long.valueOf(kv[2]);
  }
}
