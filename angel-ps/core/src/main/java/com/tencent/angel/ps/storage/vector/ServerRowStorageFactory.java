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

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.BasicTypeStorage;
import com.tencent.angel.ps.storage.vector.storage.IStorage;
import com.tencent.angel.ps.storage.vector.storage.IntArrayElementStorage;
import com.tencent.angel.ps.storage.vector.storage.IntDoubleVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntElementMapStorage;
import com.tencent.angel.ps.storage.vector.storage.IntFloatVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntIntVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.IntLongVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongDoubleVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongElementMapStorage;
import com.tencent.angel.ps.storage.vector.storage.LongFloatVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongIntVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.LongLongVectorStorage;
import com.tencent.angel.ps.storage.vector.storage.ObjectTypeStorage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Server row factory
 */
public class ServerRowStorageFactory {

  private static final Log LOG = LogFactory.getLog(ServerRowStorageFactory.class);

  /**
   * Get server row storage that store basic type elements
   *
   * @param rowType row type
   * @param startCol row split start index
   * @param endCol row split end index
   * @param estElemNum estimate element number in this split
   * @param useAdaptiveStorage use adaptive storage method or not
   * @param sparseToDenseFactor sparse storage to dense storage threshold
   * @return BasicTypeStorage basic type storage
   */
  public static BasicTypeStorage getBasicTypeStorage(RowType rowType, long startCol, long endCol,
      long estElemNum, boolean useAdaptiveKey, boolean useAdaptiveStorage, float sparseToDenseFactor) {
    BasicTypeStorage ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntDoubleVectorStorage(
              VFactory.sparseDoubleVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
        ret = new IntDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntFloatVectorStorage(
              VFactory.sparseFloatVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_DENSE_COMPONENT:
        ret = new IntFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntIntVectorStorage(
              VFactory.sparseIntVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
        ret = new IntIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)), startCol);
        break;

      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntLongVectorStorage(
              VFactory.sparseLongVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_LONG_DENSE:
      case T_LONG_DENSE_COMPONENT:
        ret = new IntLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        if (useIntKey(useAdaptiveKey, startCol, endCol)) {
          if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
            ret = new LongDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
                startCol);
          } else {
            ret = new LongDoubleVectorStorage(
                VFactory.sparseDoubleVector((int) (endCol - startCol), (int) estElemNum), startCol);
          }
        } else {
          ret = new LongDoubleVectorStorage(
              VFactory.sparseLongKeyDoubleVector(endCol - startCol, (int) estElemNum), startCol);
        }
        break;

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        ret = new LongDoubleVectorStorage(
            VFactory.sparseLongKeyDoubleVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        if (useIntKey(useAdaptiveKey, startCol, endCol)) {
          if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
            ret = new LongFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
                startCol);
          } else {
            ret = new LongFloatVectorStorage(
                VFactory.sparseFloatVector((int) (endCol - startCol), (int) estElemNum), startCol);
          }
        } else {
          ret = new LongFloatVectorStorage(
              VFactory.sparseLongKeyFloatVector(endCol - startCol, (int) estElemNum), startCol);
        }
        break;

      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        ret = new LongFloatVectorStorage(
            VFactory.sparseLongKeyFloatVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_INT_SPARSE_LONGKEY:
        if (useIntKey(useAdaptiveKey, startCol, endCol)) {
          if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
            ret = new LongIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)),
                startCol);
          } else {
            ret = new LongIntVectorStorage(
                VFactory.sparseIntVector((int) (endCol - startCol), (int) estElemNum), startCol);
          }
        } else {
          ret = new LongIntVectorStorage(
              VFactory.sparseLongKeyIntVector(endCol - startCol, (int) estElemNum), startCol);
        }
        break;

      case T_INT_SPARSE_LONGKEY_COMPONENT:
        ret = new LongIntVectorStorage(
            VFactory.sparseLongKeyIntVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_LONG_SPARSE_LONGKEY:
        if (useIntKey(useAdaptiveKey, startCol, endCol)) {
          if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
            ret = new LongLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
                startCol);
          } else {
            ret = new LongLongVectorStorage(
                VFactory.sparseLongVector((int) (endCol - startCol), (int) estElemNum), startCol);
          }
        } else {
          ret = new LongLongVectorStorage(
              VFactory.sparseLongKeyLongVector(endCol - startCol, (int) estElemNum), startCol);
        }
        break;

      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        ret = new LongLongVectorStorage(
            VFactory.sparseLongKeyLongVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        ret = new LongDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        ret = new LongFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_INT_DENSE_LONGKEY_COMPONENT:
        ret = new LongIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_LONG_DENSE_LONGKEY_COMPONENT:
        ret = new LongLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
            startCol);
        break;

      default:
        throw new UnsupportedOperationException(
            "can not support " + rowType);
    }

    return ret;
  }

  /**
   * Get server row storage that store basic type elements
   *
   * @param valueType value type class
   * @param rowType row type
   * @param startCol row split start index
   * @param endCol row split end index
   * @param estElemNum estimate element number in this split
   * @param useAdaptiveStorage use adaptive storage method or not
   * @param sparseToDenseFactor sparse storage to dense storage threshold
   * @return ObjectTypeStorage complex type storage
   */
  public static ObjectTypeStorage getComplexTypeStorage(Class<? extends IElement> valueType,
      RowType rowType, long startCol, long endCol,
      long estElemNum, boolean useAdaptiveStorage, float sparseToDenseFactor) {
    ObjectTypeStorage ret;
    switch (rowType) {
      case T_ANY_INTKEY_DENSE:
        ret = new IntArrayElementStorage(valueType, (int) (endCol - startCol), startCol);
        break;

      case T_ANY_INTKEY_SPARSE:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntArrayElementStorage(valueType, (int) (endCol - startCol), startCol);
        } else {
          ret = new IntElementMapStorage(valueType, (int) estElemNum, startCol);
        }
        break;

      case T_ANY_LONGKEY_SPARSE:
        /*if (useIntKey(useAdaptiveStorage, startCol, endCol)) {
          if (sparseToDense(true, startCol, endCol, estElemNum, sparseToDenseFactor)) {
            ret = new IntArrayElementStorage(valueType, (int) (endCol - startCol), startCol);
          } else {
            ret = new IntElementMapStorage(valueType, (int) estElemNum, startCol);
          }
        } else {
          ret = new LongElementMapStorage(valueType, (int) estElemNum, startCol);
        }*/
        ret = new LongElementMapStorage(valueType, (int) estElemNum, startCol);
        break;

      default:
        throw new UnsupportedOperationException("can not support " + rowType);
    }
    return ret;
  }

  /**
   * Get storage use storage class name
   *
   * @param storageClass storage class name
   * @return storage
   */
  public static IStorage getStorage(String storageClass) {
    try {
      Class c = Class.forName(storageClass);
      return (IStorage) c.newInstance();
    } catch (Throwable e) {
      LOG.error("load storage type " + storageClass + " failed ", e);
      throw new RuntimeException("load storage type " + storageClass + " failed ", e);
    }
  }

  private static boolean sparseToDense(boolean useAdaptiveStorage, long startCol, long endCol,
      long estElemNum, float sparseToDenseFactor) {
    return useAdaptiveStorage && (estElemNum > sparseToDenseFactor * (endCol - startCol));
  }

  private static boolean useIntKey(boolean useAdaptiveKey, long startCol, long endCol) {
    return useAdaptiveKey && (endCol - startCol <= Integer.MAX_VALUE);
  }
}
