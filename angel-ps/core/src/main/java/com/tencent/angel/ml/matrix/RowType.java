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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix;

/**
 * Model row type
 */
public enum RowType {
  /**
   * Sparse double: the element type is "double"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_DOUBLE_SPARSE(0, RowType.T_DOUBLE_SPARSE_VALUE),

  /**
   * Dense double: the element type is "double"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_DOUBLE_DENSE(1, RowType.T_DOUBLE_DENSE_VALUE),

  /**
   * Sparse int: the element type is "int"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_INT_SPARSE(2, RowType.T_INT_SPARSE_VALUE),

  /**
   * Dense int: the element type is "int", the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_INT_DENSE(3, RowType.T_INT_DENSE_VALUE),

  /**
   * Adaptive int type, it can be transfer between "T_INT_SPARSE" and "T_INT_DENSE" as the most effective memory usage
   */
  T_INT_ARBITRARY(4, RowType.T_INT_ARBITRARY_VALUE),

  /**
   * Dense float: the element type is "float"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_FLOAT_DENSE(5, RowType.T_FLOAT_DENSE_VALUE),

  /**
   * Sparse float: the element type is "float"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   */
  T_FLOAT_SPARSE(6, RowType.T_FLOAT_SPARSE_VALUE),


  /**
   * Sparse double with long type index: the element type is "double";
   * if the row dimension is in [1, Long.MAX_VALUE], the index is in [0, dimension)
   * if the row dimension is -1, the index is in [Long.MIN_VALUE, Long.MAX_VALUE]
   */
  T_DOUBLE_SPARSE_LONGKEY(7, RowType.T_DOUBLE_SPARSE_LONGKEY_VALUE),

  /**
   * Sparse double: the element type is "double"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   * This vector is contains of a batch of "T_DOUBLE_SPARSE" vectors and can be do parallel computing, so if the number of non-zero values of sparse vector is big, you can try it
   */
  T_DOUBLE_SPARSE_COMPONENT(8, RowType.T_DOUBLE_SPARSE_COMPONENT_VALUE),

  /**
   * Sparse float: the element type is "float"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   * This vector is contains of a batch of "T_FLOAT_SPARSE" vectors and can be do parallel computing, so if the number of non-zero values of sparse vector is big, you can try it
   */
  T_FLOAT_SPARSE_COMPONENT(9, RowType.T_FLOAT_SPARSE_COMPONENT_VALUE),

  /**
   * Sparse int: the element type is "int"; the index is in [0, dimension) (dimension is a positive integer, the range of dimension is [1, Integer.MAX_VALUE])
   * This vector is contains of a batch of "T_INT_SPARSE" vectors and can be do parallel computing, so if the number of non-zero values of sparse vector is big, you can try it
   */
  T_INT_SPARSE_COMPONENT(10, RowType.T_INT_SPARSE_COMPONENT_VALUE),

  /**
   * Sparse int: the element type is "double"; the index is same as "T_DOUBLE_SPARSE_LONGKEY"
   * This vector is contains of a batch of "T_DOUBLE_SPARSE_LONGKEY" vectors and can be do parallel computing, so if the number of non-zero values of sparse vector is big, you can try it
   */
  T_DOUBLE_SPARSE_LONGKEY_COMPONENT(11, RowType.T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE),

  /**
   * Sparse float: the element type is "float"; the index is same as "T_FLOAT_SPARSE_LONGKEY"
   * This vector is contains of a batch of "T_FLOAT_SPARSE_LONGKEY" vectors and can be do parallel computing, so if the number of non-zero values of sparse vector is big, you can try it
   */
  T_FLOAT_SPARSE_LONGKEY(12, RowType.T_FLOAT_SPARSE_LONGKEY_VALUE);

  public static final int T_DOUBLE_SPARSE_VALUE = 1;
  public static final int T_DOUBLE_DENSE_VALUE = 2;
  public static final int T_INT_SPARSE_VALUE = 3;
  public static final int T_INT_DENSE_VALUE = 4;
  public static final int T_INT_ARBITRARY_VALUE = 5;
  public static final int T_FLOAT_DENSE_VALUE = 6;
  public static final int T_FLOAT_SPARSE_VALUE = 7;
  public static final int T_DOUBLE_SPARSE_LONGKEY_VALUE = 8;
  public static final int T_DOUBLE_SPARSE_COMPONENT_VALUE = 9;
  public static final int T_FLOAT_SPARSE_COMPONENT_VALUE = 10;
  public static final int T_INT_SPARSE_COMPONENT_VALUE = 11;
  public static final int T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE = 12;
  public static final int T_FLOAT_SPARSE_LONGKEY_VALUE = 13;
  public static final int T_INVALID_VALUE = 14;

  public final int getNumber() {
    return value;
  }

  public static RowType valueOf(int value) {
    switch (value) {
      case T_DOUBLE_SPARSE_VALUE:
        return T_DOUBLE_SPARSE;
      case T_DOUBLE_DENSE_VALUE:
        return T_DOUBLE_DENSE;
      case T_INT_SPARSE_VALUE:
        return T_INT_SPARSE;
      case T_INT_DENSE_VALUE:
        return T_INT_DENSE;
      case T_INT_ARBITRARY_VALUE:
        return T_INT_ARBITRARY;
      case T_FLOAT_DENSE_VALUE:
        return T_FLOAT_DENSE;
      case T_FLOAT_SPARSE_VALUE:
        return T_FLOAT_SPARSE;
      case T_DOUBLE_SPARSE_LONGKEY_VALUE:
        return T_DOUBLE_SPARSE_LONGKEY;
      case T_DOUBLE_SPARSE_COMPONENT_VALUE:
        return T_DOUBLE_SPARSE_COMPONENT;
      case T_FLOAT_SPARSE_COMPONENT_VALUE:
        return T_FLOAT_SPARSE_COMPONENT;
      case T_INT_SPARSE_COMPONENT_VALUE:
        return T_INT_SPARSE_COMPONENT;
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT_VALUE:
        return T_DOUBLE_SPARSE_LONGKEY_COMPONENT;
      case T_FLOAT_SPARSE_LONGKEY_VALUE:
        return T_FLOAT_SPARSE_LONGKEY;
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
