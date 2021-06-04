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
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public class ServerRowFactory {

  public static ServerRow createEmptyServerRow(RowType type) {
    switch (type) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return new ServerIntDoubleRow(type);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return new ServerIntIntRow(type);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return new ServerIntFloatRow(type);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return new ServerIntLongRow(type);

      case T_DOUBLE_SPARSE_LONGKEY:
        return new ServerLongDoubleRow(type);

      case T_FLOAT_SPARSE_LONGKEY:
        return new ServerLongFloatRow(type);

      case T_INT_SPARSE_LONGKEY:
        return new ServerLongIntRow(type);

      case T_LONG_SPARSE_LONGKEY:
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

  public static ServerRow createServerRowRange(int rowIndex, RowType rowType, long startCol, long endCol,
      int estEleNum, Class<? extends IElement> valueClass) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return new ServerIntDoubleRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum, RouterType.RANGE);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return new ServerIntFloatRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum, RouterType.RANGE);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return new ServerIntLongRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum, RouterType.RANGE);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return new ServerIntIntRow(rowIndex, rowType, (int) startCol, (int) endCol, estEleNum, RouterType.RANGE);

      case T_DOUBLE_SPARSE_LONGKEY:
        return new ServerLongDoubleRow(rowIndex, rowType, startCol, endCol, estEleNum, RouterType.RANGE);

      case T_FLOAT_SPARSE_LONGKEY:
        return new ServerLongFloatRow(rowIndex, rowType, startCol, endCol, estEleNum, RouterType.RANGE);

      case T_LONG_SPARSE_LONGKEY:
        return new ServerLongLongRow(rowIndex, rowType, startCol, endCol, estEleNum, RouterType.RANGE);

      case T_INT_SPARSE_LONGKEY:
        return new ServerLongIntRow(rowIndex, rowType, startCol, endCol, estEleNum, RouterType.RANGE);

      case T_ANY_INTKEY_DENSE:
      case T_ANY_INTKEY_SPARSE:
        return new ServerIntAnyRow(valueClass, rowIndex, rowType, (int) startCol, (int) endCol,
            estEleNum, RouterType.RANGE);

      case T_ANY_LONGKEY_SPARSE:
        return new ServerLongAnyRow(valueClass, rowIndex, rowType, startCol, endCol,
            estEleNum, RouterType.RANGE);

      case T_ANY_STRINGKEY_SPARSE:
        return new ServerStringAnyRow(valueClass, rowIndex, rowType, (int)startCol, (int)endCol,
                estEleNum, RouterType.RANGE);

      case T_ANY_ANYKEY_SPARSE:
        return new ServerAnyAnyRow(valueClass, rowIndex, rowType, (int)startCol, (int)endCol,
                estEleNum, RouterType.RANGE);

      default:
        throw new UnsupportedOperationException("unsupport vector type:" + rowType);
    }
  }

  public static ServerRow createServerRowHash(int rowIndex, RowType rowType,
      int estEleNum, Class<? extends IElement> valueClass) {
    switch (rowType) {
      case T_DOUBLE_SPARSE:
        return new ServerIntDoubleRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_FLOAT_SPARSE:
        return new ServerIntFloatRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_LONG_SPARSE:
        return new ServerIntLongRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_INT_SPARSE:
        return new ServerIntIntRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_DOUBLE_SPARSE_LONGKEY:
        return new ServerLongDoubleRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_FLOAT_SPARSE_LONGKEY:
        return new ServerLongFloatRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_LONG_SPARSE_LONGKEY:
        return new ServerLongLongRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_INT_SPARSE_LONGKEY:
        return new ServerLongIntRow(rowIndex, rowType, 0, 0, estEleNum, RouterType.HASH);

      case T_ANY_INTKEY_SPARSE:
        return new ServerIntAnyRow(valueClass, rowIndex, rowType, 0, 0,
            estEleNum, RouterType.HASH);

      case T_ANY_LONGKEY_SPARSE:
        return new ServerLongAnyRow(valueClass, rowIndex, rowType, 0, 0,
            estEleNum, RouterType.HASH);

      case T_ANY_STRINGKEY_SPARSE:
        return new ServerStringAnyRow(valueClass, rowIndex, rowType, 0, 0,
            estEleNum, RouterType.HASH);

      case T_ANY_ANYKEY_SPARSE:
        return new ServerAnyAnyRow(valueClass, rowIndex, rowType, 0, 0,
            estEleNum, RouterType.HASH);

      default:
        throw new UnsupportedOperationException("unsupport vector type:" + rowType);
    }
  }

  public static ServerRow createServerRow(int rowIndex, RowType rowType, long startCol, long endCol,
      int estEleNum, Class<? extends IElement> valueClass, RouterType routerType) {
    if(routerType == RouterType.RANGE) {
      return createServerRowRange(rowIndex, rowType, startCol, endCol, estEleNum, valueClass);
    } else {
      return createServerRowHash(rowIndex, rowType, estEleNum, valueClass);
    }
  }
}
