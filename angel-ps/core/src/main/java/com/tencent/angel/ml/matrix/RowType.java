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


package com.tencent.angel.ml.matrix;

/**
 * Model row type
 */
public enum RowType {

  T_DOUBLE_DENSE(0, RowType.T_DOUBLE_DENSE_VALUE), T_DOUBLE_DENSE_COMPONENT(1,
    RowType.T_DOUBLE_DENSE_COMPONENT_VALUE), T_DOUBLE_DENSE_LONGKEY_COMPONENT(2,
    RowType.T_DOUBLE_DENSE_LONGKEY_COMPONENT_VALUE), T_DOUBLE_SPARSE(3,
    RowType.T_DOUBLE_SPARSE_VALUE), T_DOUBLE_SPARSE_COMPONENT(4,
    RowType.T_DOUBLE_SPARSE_COMPONENT_VALUE), T_DOUBLE_SPARSE_LONGKEY(5,
    RowType.T_DOUBLE_SPARSE_LONGKEY_VALUE), T_DOUBLE_SPARSE_LONGKEY_COMPONENT(6,
    RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE),

  T_FLOAT_DENSE(7, RowType.T_FLOAT_DENSE_VALUE), T_FLOAT_DENSE_COMPONENT(8,
    RowType.T_FLOAT_DENSE_COMPONENT_VALUE), T_FLOAT_DENSE_LONGKEY_COMPONENT(9,
    RowType.T_FLOAT_DENSE_LONGKEY_COMPONENT_VALUE), T_FLOAT_SPARSE(10,
    RowType.T_FLOAT_SPARSE_VALUE), T_FLOAT_SPARSE_COMPONENT(11,
    RowType.T_FLOAT_SPARSE_COMPONENT_VALUE), T_FLOAT_SPARSE_LONGKEY(12,
    RowType.T_FLOAT_SPARSE_LONGKEY_VALUE), T_FLOAT_SPARSE_LONGKEY_COMPONENT(13,
    RowType.T_FLOAT_SPARSE_LONGKEY_COMPONENT_VALUE),

  T_LONG_DENSE(14, RowType.T_LONG_DENSE_VALUE), T_LONG_DENSE_COMPONENT(15,
    RowType.T_LONG_DENSE_COMPONENT_VALUE), T_LONG_DENSE_LONGKEY_COMPONENT(16,
    RowType.T_LONG_DENSE_LONGKEY_COMPONENT_VALUE), T_LONG_SPARSE(17,
    RowType.T_LONG_SPARSE_VALUE), T_LONG_SPARSE_COMPONENT(18,
    RowType.T_LONG_SPARSE_COMPONENT_VALUE), T_LONG_SPARSE_LONGKEY(19,
    RowType.T_LONG_SPARSE_LONGKEY_VALUE), T_LONG_SPARSE_LONGKEY_COMPONENT(20,
    RowType.T_LONG_SPARSE_LONGKEY_COMPONENT_VALUE),

  T_INT_DENSE(21, RowType.T_INT_DENSE_VALUE), T_INT_DENSE_COMPONENT(22,
    RowType.T_INT_DENSE_COMPONENT_VALUE), T_INT_DENSE_LONGKEY_COMPONENT(23,
    RowType.T_INT_DENSE_LONGKEY_COMPONENT_VALUE), T_INT_SPARSE(24,
    RowType.T_INT_SPARSE_VALUE), T_INT_SPARSE_COMPONENT(25,
    RowType.T_INT_SPARSE_COMPONENT_VALUE), T_INT_SPARSE_LONGKEY(26,
    RowType.T_INT_SPARSE_LONGKEY_VALUE), T_INT_SPARSE_LONGKEY_COMPONENT(27,
    RowType.T_INT_SPARSE_LONGKEY_COMPONENT_VALUE),

  T_INT_ARBITRARY(28, RowType.T_INT_ARBITRARY_VALUE);


  public static final int T_DOUBLE_DENSE_VALUE = 0;
  public static final int T_DOUBLE_DENSE_COMPONENT_VALUE = 1;
  public static final int T_DOUBLE_DENSE_LONGKEY_COMPONENT_VALUE = 2;
  public static final int T_DOUBLE_SPARSE_VALUE = 3;
  public static final int T_DOUBLE_SPARSE_COMPONENT_VALUE = 4;
  public static final int T_DOUBLE_SPARSE_LONGKEY_VALUE = 5;
  public static final int T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE = 6;
  public static final int T_FLOAT_DENSE_VALUE = 7;
  public static final int T_FLOAT_DENSE_COMPONENT_VALUE = 8;
  public static final int T_FLOAT_DENSE_LONGKEY_COMPONENT_VALUE = 9;
  public static final int T_FLOAT_SPARSE_VALUE = 10;
  public static final int T_FLOAT_SPARSE_COMPONENT_VALUE = 11;
  public static final int T_FLOAT_SPARSE_LONGKEY_VALUE = 12;
  public static final int T_FLOAT_SPARSE_LONGKEY_COMPONENT_VALUE = 13;
  public static final int T_LONG_DENSE_VALUE = 14;
  public static final int T_LONG_DENSE_COMPONENT_VALUE = 15;
  public static final int T_LONG_DENSE_LONGKEY_COMPONENT_VALUE = 16;
  public static final int T_LONG_SPARSE_VALUE = 17;
  public static final int T_LONG_SPARSE_COMPONENT_VALUE = 18;
  public static final int T_LONG_SPARSE_LONGKEY_VALUE = 19;
  public static final int T_LONG_SPARSE_LONGKEY_COMPONENT_VALUE = 20;
  public static final int T_INT_DENSE_VALUE = 21;
  public static final int T_INT_DENSE_COMPONENT_VALUE = 22;
  public static final int T_INT_DENSE_LONGKEY_COMPONENT_VALUE = 23;
  public static final int T_INT_SPARSE_VALUE = 24;
  public static final int T_INT_SPARSE_COMPONENT_VALUE = 25;
  public static final int T_INT_SPARSE_LONGKEY_VALUE = 26;
  public static final int T_INT_SPARSE_LONGKEY_COMPONENT_VALUE = 27;
  public static final int T_INT_ARBITRARY_VALUE = 28;
  public static final int T_INVALID_VALUE = 29;

  public final int getNumber() {
    return value;
  }

  public static RowType valueOf(int value) {
    switch (value) {
      case T_DOUBLE_DENSE_VALUE:
        return T_DOUBLE_DENSE;
      case T_DOUBLE_DENSE_COMPONENT_VALUE:
        return T_DOUBLE_DENSE_COMPONENT;
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT_VALUE:
        return T_DOUBLE_DENSE_LONGKEY_COMPONENT;
      case T_DOUBLE_SPARSE_VALUE:
        return T_DOUBLE_SPARSE;
      case T_DOUBLE_SPARSE_COMPONENT_VALUE:
        return T_DOUBLE_SPARSE_COMPONENT;
      case T_DOUBLE_SPARSE_LONGKEY_VALUE:
        return T_DOUBLE_SPARSE_LONGKEY;
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE:
        return T_DOUBLE_SPARSE_LONGKEY_COMPONENT;
      case T_FLOAT_DENSE_VALUE:
        return T_FLOAT_DENSE;
      case T_FLOAT_DENSE_COMPONENT_VALUE:
        return T_FLOAT_DENSE_COMPONENT;
      case T_FLOAT_DENSE_LONGKEY_COMPONENT_VALUE:
        return T_FLOAT_DENSE_LONGKEY_COMPONENT;
      case T_FLOAT_SPARSE_VALUE:
        return T_FLOAT_SPARSE;
      case T_FLOAT_SPARSE_COMPONENT_VALUE:
        return T_FLOAT_SPARSE_COMPONENT;
      case T_FLOAT_SPARSE_LONGKEY_VALUE:
        return T_FLOAT_SPARSE_LONGKEY;
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT_VALUE:
        return T_FLOAT_SPARSE_LONGKEY_COMPONENT;
      case T_LONG_DENSE_VALUE:
        return T_LONG_DENSE;
      case T_LONG_DENSE_COMPONENT_VALUE:
        return T_LONG_DENSE_COMPONENT;
      case T_LONG_DENSE_LONGKEY_COMPONENT_VALUE:
        return T_LONG_DENSE_LONGKEY_COMPONENT;
      case T_LONG_SPARSE_VALUE:
        return T_LONG_SPARSE;
      case T_LONG_SPARSE_COMPONENT_VALUE:
        return T_LONG_SPARSE_COMPONENT;
      case T_LONG_SPARSE_LONGKEY_VALUE:
        return T_LONG_SPARSE_LONGKEY;
      case T_LONG_SPARSE_LONGKEY_COMPONENT_VALUE:
        return T_LONG_SPARSE_LONGKEY_COMPONENT;
      case T_INT_DENSE_VALUE:
        return T_INT_DENSE;
      case T_INT_DENSE_COMPONENT_VALUE:
        return T_INT_DENSE_COMPONENT;
      case T_INT_DENSE_LONGKEY_COMPONENT_VALUE:
        return T_INT_DENSE_LONGKEY_COMPONENT;
      case T_INT_SPARSE_VALUE:
        return T_INT_SPARSE;
      case T_INT_SPARSE_COMPONENT_VALUE:
        return T_INT_SPARSE_COMPONENT;
      case T_INT_SPARSE_LONGKEY_VALUE:
        return T_INT_SPARSE_LONGKEY;
      case T_INT_SPARSE_LONGKEY_COMPONENT_VALUE:
        return T_INT_SPARSE_LONGKEY_COMPONENT;
      case T_INT_ARBITRARY_VALUE:
        return T_INT_ARBITRARY;
      default:
        return null;
    }
  }

  private final int index;
  private final int value;

  private RowType(int index, int value) {
    this.index = index;
    this.value = value;
  }

}
