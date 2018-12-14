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

import com.tencent.angel.model.output.element.IntDoubleElement;
import com.tencent.angel.model.output.element.IntFloatElement;
import com.tencent.angel.model.output.element.IntIntElement;
import com.tencent.angel.model.output.element.IntLongElement;
import com.tencent.angel.model.output.element.LongDoubleElement;
import com.tencent.angel.model.output.element.LongFloatElement;
import com.tencent.angel.model.output.element.LongIntElement;
import com.tencent.angel.model.output.element.LongLongElement;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Text format: column id + sep + element value
 */
public class ColIdValueTextSerDe extends ElementSerDeImpl {

  private final String defaultSet = ",";
  public final static String sepParam = "text.format.filed.sep";
  private final String sep;

  public ColIdValueTextSerDe(Configuration conf) {
    super(conf);
    sep = conf.get(sepParam, defaultSet);
  }

  @Override
  public void serialize(IntFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(IntDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(IntIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(IntLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(LongFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(LongDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(LongIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void serialize(LongLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.colId) + sep + String.valueOf(element.value) + "\n");
  }

  @Override
  public void deserialize(IntFloatElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Integer.valueOf(kv[0]);
    element.value = Float.valueOf(kv[1]);
  }

  @Override
  public void deserialize(IntDoubleElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Integer.valueOf(kv[0]);
    element.value = Double.valueOf(kv[1]);
  }

  @Override
  public void deserialize(IntIntElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Integer.valueOf(kv[0]);
    element.value = Integer.valueOf(kv[1]);
  }

  @Override
  public void deserialize(IntLongElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Integer.valueOf(kv[0]);
    element.value = Long.valueOf(kv[1]);
  }

  @Override
  public void deserialize(LongFloatElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Long.valueOf(kv[0]);
    element.value = Float.valueOf(kv[1]);
  }

  @Override
  public void deserialize(LongDoubleElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Long.valueOf(kv[0]);
    element.value = Double.valueOf(kv[1]);
  }

  @Override
  public void deserialize(LongIntElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Long.valueOf(kv[0]);
    element.value = Integer.valueOf(kv[1]);
  }

  @Override
  public void deserialize(LongLongElement element, DataInputStream in) throws IOException {
    String line = in.readLine();
    String[] kv = line.split(sep);
    element.colId = Long.valueOf(kv[0]);
    element.value = Long.valueOf(kv[1]);
  }
}
