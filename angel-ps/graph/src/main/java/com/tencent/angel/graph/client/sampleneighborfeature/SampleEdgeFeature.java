package com.tencent.angel.graph.client.sampleneighborfeature;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;

public class SampleEdgeFeature extends GetFunc {

  private Long2IntOpenHashMap index;
  private LongArrayList srcs;
  private LongArrayList dsts;
  private ArrayList<IntFloatVector> edgeFeatures;
  private int edgeDim;
  private LongArrayList types;

  public SampleEdgeFeature(SampleEdgeFeatureParam param, Long2IntOpenHashMap index,
      LongArrayList srcs, LongArrayList dsts, LongArrayList types,
      ArrayList<IntFloatVector> edgeFeatures, int edgeDim) {
    super(param);
    this.index = index;
    this.srcs = srcs;
    this.dsts = dsts;
    this.edgeFeatures = edgeFeatures;
    this.edgeDim = edgeDim;
    this.types = types;
  }

  public SampleEdgeFeature(SampleEdgeFeatureParam param) {
    this(param, null, null, null, null, null, 0);
  }

  public SampleEdgeFeature() {
    this(null);
  }


  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    SampleEdgeFeaturePartParam param = (SampleEdgeFeaturePartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(param.getPartKey(), 0);
    return new SampleEdgeFeaturePartResult(param.getPartKey().getPartitionId(),
        row, param.getKeys(), param.getNumSample(), edgeDim, param.getSampleTypes());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    SampleEdgeFeatureParam param = (SampleEdgeFeatureParam) getParam();
    long[] keys = param.getKeys();

    Int2ObjectArrayMap<PartitionGetResult> partIdToResult = new Int2ObjectArrayMap<>();
    for (PartitionGetResult result : partResults) {
      partIdToResult.put(((SampleEdgeFeaturePartResult) result).getPartId(), result);
    }

    for (PartitionGetParam partParam : param.getParams()) {
      SampleEdgeFeaturePartParam param0 = (SampleEdgeFeaturePartParam) partParam;
      int start = param0.getStartIndex();
      int end = param0.getEndIndex();
      SampleEdgeFeaturePartResult result = (SampleEdgeFeaturePartResult) partIdToResult
          .get(param0.getPartKey().getPartitionId());
      int[] indptr = result.getIndptr();
      long[] neighbors = result.getNeighbors();
      IntFloatVector[] edges = result.getEdgeFeatures();

      assert indptr.length == (end - start) + 1;
      for (int i = start; i < end; i++) {
        int keyIndex = index.get(keys[i]);
        for (int j = indptr[i - start]; j < indptr[i - start + 1]; j++) {
          long n = neighbors[j];
          if (!index.containsKey(n)) {
            index.put(n, index.size());
          }
          srcs.add(keyIndex);
          dsts.add(index.get(n));
          edgeFeatures.add(edges[j]);
        }
      }
    }

    return new ScalarAggrResult(0);
  }

}
