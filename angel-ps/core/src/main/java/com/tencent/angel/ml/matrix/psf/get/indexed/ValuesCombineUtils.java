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


package com.tencent.angel.ml.matrix.psf.get.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO:add more vector type
public class ValuesCombineUtils {
  private static final Log LOG = LogFactory.getLog(ValuesCombineUtils.class);

  public static IntDoubleVector mergeSparseDoubleVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    int dim = (int) PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId())
      .getColNum();
    IntDoubleVector vector = VFactory.sparseDoubleVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
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

  public static IntFloatVector mergeSparseFloatVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    int dim = (int) PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId())
      .getColNum();
    IntFloatVector vector = VFactory.sparseFloatVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetFloatResult) part).getPartKey();
      int[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      float[] values = ((IndexPartGetFloatResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        if (i < 10) {
          LOG.debug("index " + indexes[i] + ", value " + values[i]);
        }
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static IntIntVector mergeSparseIntVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    int dim = (int) PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId())
      .getColNum();
    IntIntVector vector = VFactory.sparseIntVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
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

  public static IntLongVector mergeSparseLongVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    int dim = (int) PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId())
      .getColNum();
    IntLongVector vector = VFactory.sparseLongVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetLongResult) part).getPartKey();
      int[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      long[] values = ((IndexPartGetLongResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        if (i < 10) {
          LOG.debug("merge index = " + indexes[i] + ", value = " + values[i]);
        }
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  // TODO : subDim set
  public static CompIntDoubleVector mergeSparseDoubleCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    int dim = (int) meta.getColNum();
    int subDim = (int) meta.getBlockColNum();

    int size = partKeys.size();
    IntDoubleVector[] splitVecs = new IntDoubleVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        double[] values =
          ((IndexPartGetDoubleResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        int[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseDoubleVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseDoubleVector(subDim, 0);
      }
    }

    CompIntDoubleVector vector = VFactory.compIntDoubleVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  private static void transformIndices(int[] indices, PartitionKey partKey) {
    for (int i = 0; i < indices.length; i++) {
      indices[i] -= partKey.getStartCol();
    }
  }

  private static Map<PartitionKey, PartitionGetResult> mapPartKeyToResult(
    List<PartitionGetResult> partResults) {
    int size = partResults.size();
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      partKeyToResultMap
        .put(((IndexPartGetResult) partResults.get(i)).getPartKey(), partResults.get(i));
    }
    return partKeyToResultMap;
  }

  private static List<PartitionKey> getSortedPartKeys(int matrixId, int rowIndex) {
    List<PartitionKey> partKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    partKeys.sort((PartitionKey part1, PartitionKey part2) -> {
      return (int) (part1.getStartCol() - part2.getStartCol());
    });
    return partKeys;
  }

  public static CompIntIntVector mergeSparseIntCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    int dim = (int) meta.getColNum();
    int subDim = (int) meta.getBlockColNum();

    int size = partKeys.size();
    IntIntVector[] splitVecs = new IntIntVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        int[] values =
          ((IndexPartGetIntResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        int[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseIntVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseIntVector(subDim, 0);
      }
    }

    CompIntIntVector vector = VFactory.compIntIntVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompIntLongVector mergeSparseLongCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    int dim = (int) meta.getColNum();
    int subDim = (int) meta.getBlockColNum();

    int size = partKeys.size();
    IntLongVector[] splitVecs = new IntLongVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        long[] values =
          ((IndexPartGetLongResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        int[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseLongVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseLongVector(subDim, 0);
      }
    }

    CompIntLongVector vector = VFactory.compIntLongVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompIntFloatVector mergeSparseFloatCompVector(IndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    int dim = (int) meta.getColNum();
    int subDim = (int) meta.getBlockColNum();

    int size = partKeys.size();
    IntFloatVector[] splitVecs = new IntFloatVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        float[] values =
          ((IndexPartGetFloatResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        int[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseFloatVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseFloatVector(subDim, 0);
      }
    }

    CompIntFloatVector vector = VFactory.compIntFloatVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static LongDoubleVector mergeSparseDoubleVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    long dim =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum();
    LongDoubleVector vector = VFactory.sparseLongKeyDoubleVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetResult) part).getPartKey();
      long[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      double[] values = ((IndexPartGetDoubleResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static LongFloatVector mergeSparseFloatVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    long dim =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum();
    LongFloatVector vector = VFactory.sparseLongKeyFloatVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetResult) part).getPartKey();
      long[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      float[] values = ((IndexPartGetFloatResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static LongIntVector mergeSparseIntVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    long dim =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum();
    LongIntVector vector = VFactory.sparseLongKeyIntVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetResult) part).getPartKey();
      long[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      int[] values = ((IndexPartGetIntResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static LongLongVector mergeSparseLongVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    long dim =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getColNum();
    LongLongVector vector = VFactory.sparseLongKeyLongVector(dim, param.size());

    for (PartitionGetResult part : partResults) {
      PartitionKey partKey = ((IndexPartGetResult) part).getPartKey();
      long[] indexes = param.getPartKeyToIndexesMap().get(partKey);
      long[] values = ((IndexPartGetLongResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }

    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompLongDoubleVector mergeSparseDoubleCompVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    long dim = meta.getColNum();
    long subDim = meta.getBlockColNum();

    int size = partKeys.size();
    LongDoubleVector[] splitVecs = new LongDoubleVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        double[] values =
          ((IndexPartGetDoubleResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        long[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseLongKeyDoubleVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseLongKeyDoubleVector(subDim, 1);
      }
    }

    CompLongDoubleVector vector = VFactory.compLongDoubleVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompLongFloatVector mergeSparseFloatCompVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    long dim = meta.getColNum();
    long subDim = meta.getBlockColNum();

    int size = partKeys.size();
    LongFloatVector[] splitVecs = new LongFloatVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        float[] values =
          ((IndexPartGetFloatResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        long[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseLongKeyFloatVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseLongKeyFloatVector(subDim, 1);
      }
    }

    CompLongFloatVector vector = VFactory.compLongFloatVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompLongIntVector mergeSparseIntCompVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    long dim = meta.getColNum();
    long subDim = meta.getBlockColNum();

    int size = partKeys.size();
    LongIntVector[] splitVecs = new LongIntVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        int[] values =
          ((IndexPartGetIntResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        long[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseLongKeyIntVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseLongKeyIntVector(subDim, 0);
      }
    }

    CompLongIntVector vector = VFactory.compLongIntVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  public static CompLongLongVector mergeSparseLongCompVector(LongIndexGetParam param,
    List<PartitionGetResult> partResults) {
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = mapPartKeyToResult(partResults);
    List<PartitionKey> partKeys = getSortedPartKeys(param.matrixId, param.getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);
    long dim = meta.getColNum();
    long subDim = meta.getBlockColNum();

    int size = partKeys.size();
    LongLongVector[] splitVecs = new LongLongVector[size];
    for (int i = 0; i < size; i++) {
      if (param.getPartKeyToIndexesMap().containsKey(partKeys.get(i))) {
        long[] values =
          ((IndexPartGetLongResult) partKeyToResultMap.get(partKeys.get(i))).getValues();
        long[] indices = param.getPartKeyToIndexesMap().get(partKeys.get(i));
        transformIndices(indices, partKeys.get(i));
        splitVecs[i] = VFactory.sparseLongKeyLongVector(subDim, indices, values);
      } else {
        splitVecs[i] = VFactory.sparseLongKeyLongVector(subDim, 0);
      }
    }

    CompLongLongVector vector = VFactory.compLongLongVector(dim, splitVecs, subDim);
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(param.getRowId());
    return vector;
  }

  private static void transformIndices(long[] indices, PartitionKey partKey) {
    for (int i = 0; i < indices.length; i++) {
      indices[i] -= partKey.getStartCol();
    }
  }
}
