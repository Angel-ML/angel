package com.tencent.angel.graph.utils;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerAnyAnyRow;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;

public class GraphMatrixUtils {
  public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext, PartitionKey partKey) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partKey.getMatrixId());
    ServerPartition part = matrix.getPartition(partKey.getPartitionId());
    return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
      PartitionGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
      PartitionUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
      GeneralPartGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerLongIntRow getPSLongKeyIntRow(PSContext psContext,
                                                    PartitionGetParam partParam, int rowId) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerLongIntRow) (((RowBasedPartition) part).getRow(rowId));
  }

  public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
      GeneralPartUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext, PartitionKey partKey) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partKey.getMatrixId());
    ServerPartition part = matrix.getPartition(partKey.getPartitionId());
    return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
      PartitionGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
      PartitionUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
      GeneralPartGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
      GeneralPartUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerAnyAnyRow getPSAnyKeyRow(PSContext psContext,
      PartitionUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerAnyAnyRow) (((RowBasedPartition) part).getRow(0));
  }

  public static ServerAnyAnyRow getPSAnyKeyRow(PSContext psContext,
      PartitionGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    return (ServerAnyAnyRow) (((RowBasedPartition) part).getRow(0));
  }
}
