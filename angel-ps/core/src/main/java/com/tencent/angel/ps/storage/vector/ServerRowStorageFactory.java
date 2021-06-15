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
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.*;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
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
      long estElemNum, boolean useAdaptiveKey, boolean useAdaptiveStorage, float sparseToDenseFactor,
      RouterType routerType) {
    if(routerType == RouterType.RANGE) {
      return getBasicTypeStorageRange(rowType, startCol, endCol, estElemNum, useAdaptiveKey,
          useAdaptiveStorage, sparseToDenseFactor);
    } else {
      return getBasicTypeStorageHash(rowType, estElemNum);
    }
  }

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
  public static BasicTypeStorage getBasicTypeStorageRange(RowType rowType, long startCol, long endCol,
      long estElemNum, boolean useAdaptiveKey, boolean useAdaptiveStorage, float sparseToDenseFactor) {
    BasicTypeStorage ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntDoubleVectorStorage(
              VFactory.sparseDoubleVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_DOUBLE_DENSE:
        ret = new IntDoubleVectorStorage(VFactory.denseDoubleVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_FLOAT_SPARSE:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntFloatVectorStorage(
              VFactory.sparseFloatVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_FLOAT_DENSE:
        ret = new IntFloatVectorStorage(VFactory.denseFloatVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_INT_SPARSE:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntIntVectorStorage(
              VFactory.sparseIntVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_INT_DENSE:
        ret = new IntIntVectorStorage(VFactory.denseIntVector((int) (endCol - startCol)), startCol);
        break;

      case T_LONG_SPARSE:
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
              startCol);
        } else {
          ret = new IntLongVectorStorage(
              VFactory.sparseLongVector((int) (endCol - startCol), (int) estElemNum), startCol);
        }
        break;

      case T_LONG_DENSE:
        ret = new IntLongVectorStorage(VFactory.denseLongVector((int) (endCol - startCol)),
            startCol);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        ret = new LongDoubleVectorStorage(
            VFactory.sparseLongKeyDoubleVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        ret = new LongFloatVectorStorage(
            VFactory.sparseLongKeyFloatVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_INT_SPARSE_LONGKEY:
        ret = new LongIntVectorStorage(
            VFactory.sparseLongKeyIntVector(endCol - startCol, (int) estElemNum), startCol);
        break;

      case T_LONG_SPARSE_LONGKEY:
        ret = new LongLongVectorStorage(
            VFactory.sparseLongKeyLongVector(endCol - startCol, (int) estElemNum), startCol);
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
   * @param rowType row type
   * @param estElemNum estimate element number in this split
   * @return BasicTypeStorage basic type storage
   */
  public static BasicTypeStorage getBasicTypeStorageHash(RowType rowType, long estElemNum) {
    BasicTypeStorage ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
        ret = new IntDoubleVectorStorage(
            VFactory.sparseDoubleVector(-1, (int) estElemNum), 0);
        break;

      case T_FLOAT_SPARSE:
        ret = new IntFloatVectorStorage(
            VFactory.sparseFloatVector(-1, (int) estElemNum), 0);
        break;

      case T_INT_SPARSE:
        ret = new IntIntVectorStorage(
            VFactory.sparseIntVector(-1, (int) estElemNum), 0);
        break;

      case T_LONG_SPARSE:
        ret = new IntLongVectorStorage(
            VFactory.sparseLongVector(-1, (int) estElemNum), 0);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        ret = new LongDoubleVectorStorage(
            VFactory.sparseLongKeyDoubleVector(-1, (int) estElemNum), 0);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        ret = new LongFloatVectorStorage(
            VFactory.sparseLongKeyFloatVector(-1, (int) estElemNum), 0);
        break;

      case T_INT_SPARSE_LONGKEY:
        ret = new LongIntVectorStorage(
            VFactory.sparseLongKeyIntVector(-1, (int) estElemNum), 0);
        break;

      case T_LONG_SPARSE_LONGKEY:
        ret = new LongLongVectorStorage(
            VFactory.sparseLongKeyLongVector(-1, (int) estElemNum), 0);
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
      long estElemNum, boolean useAdaptiveStorage, float sparseToDenseFactor, RouterType routerType) {
    if(routerType == RouterType.RANGE) {
      return getComplexTypeStorageRange(valueType, rowType, startCol, endCol,
          estElemNum, useAdaptiveStorage, sparseToDenseFactor);
    } else {
      return getComplexTypeStorageHash(valueType, rowType, estElemNum);
    }
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
  public static ObjectTypeStorage getComplexTypeStorageRange(Class<? extends IElement> valueType,
      RowType rowType, long startCol, long endCol,
      long estElemNum, boolean useAdaptiveStorage, float sparseToDenseFactor) {
    ObjectTypeStorage ret;
    switch (rowType) {
      case T_ANY_INTKEY_DENSE:
        ret = new IntArrayElementStorage(valueType, (int) (endCol - startCol), startCol);
        break;

      case T_ANY_INTKEY_SPARSE:
        /*
        if (sparseToDense(useAdaptiveStorage, startCol, endCol, estElemNum, sparseToDenseFactor)) {
          ret = new IntArrayElementStorage(valueType, (int) (endCol - startCol), startCol);
        } else {
          ret = new IntElementMapStorage(valueType, (int) estElemNum, startCol);
        }
        */
        LOG.debug("estElemNum is: " + estElemNum + ", startCol is: " + startCol);
        ret = new IntElementMapStorage(valueType, (int) estElemNum, startCol);
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
        LOG.debug("estElemNum is: " + estElemNum + " startCol is: " + startCol);
        ret = new LongElementMapStorage(valueType, (int) estElemNum, startCol);
        break;

      case T_ANY_STRINGKEY_SPARSE:
        LOG.debug("estElemNum is: " + estElemNum + " startCol is: " + startCol);
        ret = new StringElementMapStorage(valueType, (int) estElemNum, startCol);
        break;

      case T_ANY_ANYKEY_SPARSE:
        LOG.debug("estElemNum is: " + estElemNum + " startCol is: " + startCol);
        ret = new ElementElementMapStorage(valueType, (int) estElemNum, startCol);
        break;

      default:
        throw new UnsupportedOperationException("can not support " + rowType);
    }
    return ret;
  }

  /**
   * Get server row storage that store basic type elements
   *
   * @param valueType value type class
   * @param rowType row type
   * @param estElemNum estimate element number in this split
   * @return ObjectTypeStorage complex type storage
   */
  public static ObjectTypeStorage getComplexTypeStorageHash(Class<? extends IElement> valueType,
      RowType rowType, long estElemNum) {
    ObjectTypeStorage ret;
    switch (rowType) {
      case T_ANY_INTKEY_SPARSE:
        ret = new IntElementMapStorage(valueType, (int) estElemNum, 0);
        break;

      case T_ANY_LONGKEY_SPARSE:
        ret = new LongElementMapStorage(valueType, (int) estElemNum, 0);
        break;

      case T_ANY_STRINGKEY_SPARSE:
        ret = new StringElementMapStorage(valueType, (int) estElemNum, 0);
        break;

      case T_ANY_ANYKEY_SPARSE:
        ret = new ElementElementMapStorage(valueType, (int) estElemNum, 0);
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
