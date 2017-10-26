/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.tools;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;

/**
 * Text serializer
 */
public class TextModelLineConvert implements ModelLineConvert {
  private final String seperator;
  
  public TextModelLineConvert() {
    seperator = ":";
  }

  public TextModelLineConvert(String seperator) {
    this.seperator = seperator;
  }

  @Override public void convertRowIndex(FSDataOutputStream output, int rowIndex) throws
    IOException {
    output.writeBytes("rowindex=" + rowIndex + "\n");
  }

  @Override public void convertDouble(FSDataOutputStream output, int index, double value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertFloat(FSDataOutputStream output, int index, float value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertInt(FSDataOutputStream output, int index, float value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }

  @Override public void convertDoubleLongKey(FSDataOutputStream output, long index, double value)
    throws IOException {
    output.writeBytes(index + ":" + value + "\n");
  }
}
