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


package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;

public class ServerRowFactory {

  public static ServerRow createEmptyServerRow(RowType type) {
    switch (type) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return new ServerIntDoubleRow(type);

      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return new ServerIntIntRow(type);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return new ServerIntFloatRow(type);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return new ServerIntLongRow(type);

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        return new ServerLongDoubleRow(type);

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        return new ServerLongFloatRow(type);

      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
      case T_INT_DENSE_LONGKEY_COMPONENT:
        return new ServerLongIntRow(type);

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
        return new ServerLongLongRow(type);

      case T_ANY_INTKEY_DENSE:
      case T_ANY_INTKEY_SPARSE:
        return new ServerIntAnyRow(type);

      case T_ANY_LONGKEY_SPARSE:
        return new ServerLongAnyRow(type);

      default:
        throw new UnsupportedOperationException("unsupport row type " + type);
    }
  }

  public static ServerRow createServerRow(int rowIndex, RowType rowType, long startCol, long endCol,
      int estEleNum, Class<? extends IElement> valueClass) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        return new ServerIntDoubleRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum);

      case T_FLOAT_DENSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        return new ServerIntFloatRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum);

      case T_LONG_DENSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        return new ServerIntLongRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum);

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        return new ServerIntIntRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum);

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        return new ServerLongDoubleRow(rowIndex, rowType, startCol, endCol, estEleNum);

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        return new ServerLongFloatRow(rowIndex, rowType, startCol, endCol, estEleNum);

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
        return new ServerLongLongRow(rowIndex, rowType, startCol, endCol, estEleNum);

      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
      case T_INT_DENSE_LONGKEY_COMPONENT:
        return new ServerLongIntRow(rowIndex, rowType, startCol, endCol, estEleNum);

      case T_ANY_INTKEY_DENSE:
      case T_ANY_INTKEY_SPARSE:
        return new ServerIntAnyRow(valueClass, rowIndex, rowType, (int) startCol, (int) endCol,
            estEleNum);

      case T_ANY_LONGKEY_SPARSE:
        return new ServerLongAnyRow(valueClass, rowIndex, rowType, (int) startCol, (int) endCol,
            estEleNum);

      default:
        throw new UnsupportedOperationException("unsupport vector type:" + rowType);
    }
  }
}
