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


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.matrix.RowType;

public class MatrixFactory {
  public static Matrix createRBMatrix(RowType rowType, int rowNum, long colNum, long subDim) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return MFactory.rbIntDoubleMatrix(rowNum, (int) colNum);

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return MFactory.rbCompIntDoubleMatrix(rowNum, (int) colNum, (int) subDim);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return MFactory.rbIntFloatMatrix(rowNum, (int) colNum);

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return MFactory.rbCompIntFloatMatrix(rowNum, (int) colNum, (int) subDim);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return MFactory.rbIntIntMatrix(rowNum, (int) colNum);

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return MFactory.rbCompIntIntMatrix(rowNum, (int) colNum, (int) subDim);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return MFactory.rbIntLongMatrix(rowNum, (int) colNum);

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return MFactory.rbCompIntLongMatrix(rowNum, (int) colNum, (int) subDim);

      case T_DOUBLE_SPARSE_LONGKEY:
        return MFactory.rbLongDoubleMatrix(rowNum, colNum);

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbCompLongDoubleMatrix(rowNum, colNum, subDim);


      case T_FLOAT_SPARSE_LONGKEY:
        return MFactory.rbLongFloatMatrix(rowNum, colNum);

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return MFactory.rbCompLongFloatMatrix(rowNum, colNum, subDim);

      case T_INT_SPARSE_LONGKEY:
        return MFactory.rbLongIntMatrix(rowNum, colNum);

      case T_INT_SPARSE_LONGKEY_COMPONENT:
      case T_INT_DENSE_LONGKEY_COMPONENT:
        return MFactory.rbCompLongIntMatrix(rowNum, colNum, subDim);

      case T_LONG_SPARSE_LONGKEY:
        return MFactory.rbLongLongMatrix(rowNum, colNum);

      case T_LONG_SPARSE_LONGKEY_COMPONENT:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
        return MFactory.rbCompLongLongMatrix(rowNum, colNum, subDim);

      default:
        return null;
    }
  }
}
