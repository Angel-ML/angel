package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValuesCombineUtils {
  public static SparseDoubleVector mergeSparseDoubleVector(IndexGetParam param, List<PartitionGetResult> partResults) {
    SparseDoubleVector vector = new SparseDoubleVector(
      (int) PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum(),
      param.size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((IndexPartGetDoubleResult) part).getPartKey();
      int[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      double[] values = ((IndexPartGetDoubleResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static SparseFloatVector mergeSparseFloatVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    SparseFloatVector vector = new SparseFloatVector(
      (int)PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum(),
      param.size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((IndexPartGetFloatResult) part).getPartKey();
      int[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      float[] values = ((IndexPartGetFloatResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static SparseIntVector mergeSparseIntVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    SparseIntVector vector = new SparseIntVector(
      (int)PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum(),
      param.size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((IndexPartGetIntResult) part).getPartKey();
      int[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      int[] values = ((IndexPartGetIntResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompSparseDoubleVector mergeSparseDoubleCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    int size = partKeys.size();
    SparseDoubleVector [] splitVecs = new SparseDoubleVector[size];
    for(int i = 0; i < size; i++) {
      if(param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseDoubleVector((int)meta.getColNum(),
          param.getPartKeyToIndexesMap().get(partKeys.get(i)),
          ((IndexPartGetDoubleResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    return new CompSparseDoubleVector(meta.getId(),
      param.getRowId(), (int)meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
  }

  private static Map<PartitionKey, PartitionGetResult> mapPartKeyToResult(List<PartitionGetResult> partResults) {
    int size = partResults.size();
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = new HashMap<>(size);
    for(int i = 0; i < size; i++) {
      if(partResults.get(i) instanceof IndexPartGetResult) {
        partKeyToResultMap.put(((IndexPartGetResult)partResults.get(i)).getPartKey(), partResults.get(i));
      } else {
        partKeyToResultMap.put(((LongIndexGetResult)partResults.get(i)).getPartKey(), partResults.get(i));
      }
    }
    return partKeyToResultMap;
  }

  private static List<PartitionKey> getSortedPartKeys(int matrixId, int rowIndex) {
    List<PartitionKey> partKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(
      matrixId, rowIndex);
    partKeys.sort((PartitionKey part1, PartitionKey part2) -> {
      return (int)(part1.getStartCol() - part2.getStartCol());
    });
    return partKeys;
  }

  public static CompSparseIntVector mergeSparseIntCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    int size = partKeys.size();
    SparseIntVector [] splitVecs = new SparseIntVector[size];
    for(int i = 0; i < size; i++) {
      if(param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseIntVector((int)meta.getColNum(),
          param.getPartKeyToIndexesMap().get(partKeys.get(i)),
          ((IndexPartGetIntResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    return new CompSparseIntVector(meta.getId(),
      param.getRowId(), (int)meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
  }

  public static CompSparseFloatVector mergeSparseFloatCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    int size = partKeys.size();
    SparseFloatVector [] splitVecs = new SparseFloatVector[size];
    for(int i = 0; i < size; i++) {
      if(param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseFloatVector((int)meta.getColNum(),
          param.getPartKeyToIndexesMap().get(partKeys.get(i)),
          ((IndexPartGetFloatResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    return new CompSparseFloatVector(meta.getId(),
      param.getRowId(), (int)meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
  }

  public static SparseLongKeyDoubleVector mergeSparseDoubleVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    SparseLongKeyDoubleVector vector = new SparseLongKeyDoubleVector(
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum(),
      param.size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((LongIndexGetResult) part).partKey;
      long[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      double[] values = ((LongIndexGetResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompSparseLongKeyDoubleVector mergeSparseDoubleCompVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    int size = partKeys.size();
    SparseLongKeyDoubleVector [] splitVecs = new SparseLongKeyDoubleVector[size];
    for(int i = 0; i < size; i++) {
      if(param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseLongKeyDoubleVector(meta.getColNum(),
          param.getPartKeyToIndexesMap().get(partKeys.get(i)),
          ((LongIndexGetResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    CompSparseLongKeyDoubleVector vector = new CompSparseLongKeyDoubleVector(meta.getId(),
      param.getRowId(), meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
    return vector;
  }
}
