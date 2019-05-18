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

import com.tencent.angel.ml.matrix.RowType;

public class RowUpdateSplitFactory {

  public static RowUpdateSplit get(RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
        return new DenseDoubleRowUpdateSplit();

      case T_DOUBLE_SPARSE:
        return new SparseDoubleRowUpdateSplit();

      case T_FLOAT_DENSE:
        return new DenseFloatRowUpdateSplit();

      case T_FLOAT_SPARSE:
        return new SparseFloatRowUpdateSplit();

      case T_INT_DENSE:
        return new DenseIntRowUpdateSplit();

      case T_INT_SPARSE:
        return new SparseIntRowUpdateSplit();

      case T_LONG_DENSE:
        return new DenseLongRowUpdateSplit();

      case T_LONG_SPARSE:
        return new SparseLongRowUpdateSplit();

      case T_DOUBLE_SPARSE_LONGKEY:
        return new LongKeySparseDoubleRowUpdateSplit();

      case T_FLOAT_SPARSE_LONGKEY:
        return new LongKeySparseFloatRowUpdateSplit();

      case T_INT_SPARSE_LONGKEY:
        return new LongKeySparseIntRowUpdateSplit();

      case T_LONG_SPARSE_LONGKEY:
        return new LongKeySparseLongRowUpdateSplit();

      default:
        throw new UnsupportedOperationException("Unknown row type " + rowType);
    }
  }
}
