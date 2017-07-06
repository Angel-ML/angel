/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.graphx;

public enum DataType {
  Unknown((short)-1),

  Integer((short)0),

  Double((short)1),

  Float((short)2),

  Long((short)3);

  private final short value;

  private DataType(short value) {
    this.value = value;
  }

  public short value() {
    return value;
  }

  public static DataType valueOf(short value) {
    for (DataType type : DataType.values()) {
      if (type.value() == value) {
        return type;
      }
    }
    throw new IllegalArgumentException("Invalid value.");
  }

  public int size() {
    switch (this) {
      case Integer:
        return 4;
      case Double:
        return 8;
      case Float:
        return 4;
      case Long:
        return 8;
      default:
        return -1;
    }
  }
}
