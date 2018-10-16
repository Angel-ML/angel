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
 * Text format: values without index
 */
public class ValueTextRowFormat extends RowFormat {
  @Override public void save(IntFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(IntDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(IntIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(IntLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(LongFloatElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(LongDoubleElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(LongIntElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void save(LongLongElement element, DataOutputStream out) throws IOException {
    out.writeBytes(String.valueOf(element.value) + "\n");
  }

  @Override public void load(IntFloatElement element, DataInputStream in) throws IOException {
    element.value = Float.valueOf(in.readLine());
  }

  @Override public void load(IntDoubleElement element, DataInputStream in) throws IOException {
    element.value = Double.valueOf(in.readLine());
  }

  @Override public void load(IntIntElement element, DataInputStream in) throws IOException {
    element.value = Integer.valueOf(in.readLine());
  }

  @Override public void load(IntLongElement element, DataInputStream in) throws IOException {
    element.value = Long.valueOf(in.readLine());
  }

  @Override public void load(LongFloatElement element, DataInputStream in) throws IOException {
    element.value = Float.valueOf(in.readLine());
  }

  @Override public void load(LongDoubleElement element, DataInputStream in) throws IOException {
    element.value = Double.valueOf(in.readLine());
  }

  @Override public void load(LongIntElement element, DataInputStream in) throws IOException {
    element.value = Integer.valueOf(in.readLine());
  }

  @Override public void load(LongLongElement element, DataInputStream in) throws IOException {
    element.value = Long.valueOf(in.readLine());
  }
}
