package com.tencent.angel.graph.utils;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.common.param.ModelContext;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partition.UserDefinePartition;
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner;
import com.tencent.angel.ps.storage.partitioner.HashPartitioner;
import com.tencent.angel.ps.storage.vector.element.IElement;

public class ModelContextUtils {

  public static MatrixContext createMatrixContext(ModelContext context, RowType rowType) {
    return createMatrixContext(context, rowType, null);
  }

  public static MatrixContext createMatrixContext(ModelContext context, RowType rowType,
      int rowNum) {
    return createMatrixContext(context, rowType, null, rowNum);
  }

  public static MatrixContext createMatrixContext(ModelContext context, RowType rowType,
      Class<? extends IElement> elemClass) {
    return createMatrixContext(context, rowType, elemClass, 1);
  }

  public static MatrixContext createMatrixContext(ModelContext context, RowType rowType,
      Class<? extends IElement> elemClass, int rowNum) {
    if (rowType.isComplexValue() && elemClass == null) {
      throw new InvalidParameterException("Complex value type must set element class type");
    }

    MatrixContext mc = new MatrixContext();
    mc.setName(context.getModelName());
    mc.setRowNum(rowNum);
    mc.setRowType(rowType);
    mc.setPartitionNum(context.getPartitionNum());
    mc.setValidIndexNum(context.getNodeNum());

    if (elemClass != null) {
      mc.setValueType(elemClass);
    }

    if (context.isUseHashPartition()) {
      mc.setPartitionerClass(HashPartitioner.class);
    } else {
      mc.setIndexStart(context.getMinNodeId());
      mc.setIndexEnd(context.getMaxNodeId());
      mc.setPartitionerClass(ColumnRangePartitioner.class);
      if (context.getPartitionNum() > 0) {
        mc.setMaxRowNumInBlock(rowNum);
        mc.setMaxColNumInBlock(
            (context.getMaxNodeId() - context.getMinNodeId()) / context.getPartitionNum());
      }
    }

    return mc;
  }


  public static MatrixContext createMatrixContextWithUserDefinePartition(ModelContext context, String name,
      RowType rowType, Class<? extends UserDefinePartition> partClass) {
    MatrixContext mc = new MatrixContext();
    mc.setName(name);
    mc.setRowNum(1);
    mc.setRowType(rowType);
    mc.setPartitionNum(context.getPartitionNum());
    mc.setValidIndexNum(context.getNodeNum());

    if (partClass != null) {
      mc.setPartitionClass(partClass);
    }

    if (context.isUseHashPartition()) {
      mc.setPartitionerClass(HashPartitioner.class);
    } else {
      mc.setIndexStart(context.getMinNodeId());
      mc.setIndexEnd(context.getMaxNodeId());
      mc.setPartitionerClass(ColumnRangePartitioner.class);
      if (context.getPartitionNum() > 0) {
        mc.setMaxRowNumInBlock(1);
        mc.setMaxColNumInBlock(
            (context.getMaxNodeId() - context.getMinNodeId()) / context.getPartitionNum());
      }
    }

    return mc;
  }

  public static MatrixContext createMatrixContext(ModelContext context, String name,
      RowType rowType) {
    return createMatrixContext(context, name, rowType, null);
  }

  public static MatrixContext createMatrixContext(ModelContext context, String name,
      RowType rowType, int rowNum) {
    return createMatrixContext(context, name, rowType, null, rowNum);
  }

  public static MatrixContext createMatrixContext(ModelContext context, String name,
      RowType rowType, Class<? extends IElement> elemClass) {
    return createMatrixContext(context, name, rowType, elemClass, 1);
  }

  public static MatrixContext createMatrixContext(ModelContext context, String name,
      RowType rowType, Class<? extends IElement> elemClass, int rowNum) {
    if (rowType.isComplexValue() && elemClass == null) {
      throw new InvalidParameterException("Complex value type must set element class type");
    }

    MatrixContext mc = new MatrixContext();
    mc.setName(name);
    mc.setRowNum(rowNum);
    mc.setRowType(rowType);
    mc.setPartitionNum(context.getPartitionNum());
    mc.setValidIndexNum(context.getNodeNum());

    if (elemClass != null) {
      mc.setValueType(elemClass);
    }

    if (context.isUseHashPartition()) {
      mc.setPartitionerClass(HashPartitioner.class);
    } else {
      mc.setIndexStart(context.getMinNodeId());
      mc.setIndexEnd(context.getMaxNodeId());
      mc.setPartitionerClass(ColumnRangePartitioner.class);
      if (context.getPartitionNum() > 0) {
        mc.setMaxRowNumInBlock(1);
        mc.setMaxColNumInBlock(
            (context.getMaxNodeId() - context.getMinNodeId()) / context.getPartitionNum());
      }
    }

    return mc;
  }
}
