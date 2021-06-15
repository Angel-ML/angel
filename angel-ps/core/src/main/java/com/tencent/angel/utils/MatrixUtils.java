package com.tencent.angel.utils;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.exception.ObjectNotFoundException;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.storage.MatrixStorageManager;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;

public class MatrixUtils {

  public static ServerRow getRow(MatrixStorageManager storageManager, int matrixId, int partId,
      int rowId) {
    ServerMatrix matrix = storageManager.getMatrix(matrixId);
    if (matrix == null) {
      throw new ObjectNotFoundException("Can not find matrix " + matrixId);
    }

    ServerPartition part = matrix.getPartition(partId);
    if (part == null) {
      throw new ObjectNotFoundException(
          "Can not find partition " + partId + " of matrix " + matrixId);
    }

    if (!(part instanceof RowBasedPartition)) {
      throw new UnsupportedOperationException("Get row only support for RowBasedPartition");
    }

    ServerRow row = ((RowBasedPartition) part).getRow(rowId);
    if (row == null) {
      throw new ObjectNotFoundException(
          "Can not find row " + rowId + " of matrix " + matrixId + " in partition " + partId);
    }

    return row;
  }

  public static ServerPartition getPart(MatrixStorageManager storageManager, int matrixId,
      int partId) {
    ServerMatrix matrix = storageManager.getMatrix(matrixId);
    if (matrix == null) {
      throw new ObjectNotFoundException("Can not find matrix " + matrixId);
    }

    ServerPartition part = matrix.getPartition(partId);
    if (part == null) {
      throw new ObjectNotFoundException(
          "Can not find partition " + partId + " of matrix " + matrixId);
    }
    return part;
  }

  public static ValueType getValueType(RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        return ValueType.DOUBLE;

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        return ValueType.FLOAT;

      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
      case T_INT_DENSE_LONGKEY_COMPONENT:
        return ValueType.INT;

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
        return ValueType.LONG;

      default:
        return ValueType.DOUBLE;
    }
  }

  public static int getValueSize(ValueType valueType) {
    if (valueType == ValueType.DOUBLE || valueType == ValueType.LONG) {
      return 8;
    } else {
      return 4;
    }
  }
}
