package com.tencent.angel.graph.client.sampleFeats;

import com.tencent.angel.graph.client.getnodefeats2.PartGetNodeFeatsResult;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SampleNodeFeats extends GetFunc {

  public SampleNodeFeats(SampleNodeFeatsParam param) {
    super(param);
  }

  public SampleNodeFeats() {
    this((SampleNodeFeatsParam) null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNodeFeatsParam param = (PartSampleNodeFeatsParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));

    int size = Math.min(row.size(), param.getSize());
    IntFloatVector[] feats = new IntFloatVector[size];
    Random rand = new Random(System.currentTimeMillis());
    // sample continuously beginning from a random index
    int bound = row.size() - size;
    int skip = bound > 0 ? rand.nextInt(bound) : 0;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = row.getStorage().iterator();
    it.skip(skip);
    for (int i = 0; i < size; i++) {
      feats[i] = ((Node) it.next().getValue()).getFeats();
    }
    return new PartGetNodeFeatsResult(part.getPartitionKey().getPartitionId(), feats);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<IntFloatVector> results = new ArrayList<>();
    for (PartitionGetResult partResult : partResults) {
      results.addAll(Arrays.asList(((PartGetNodeFeatsResult) partResult).getFeats()));
    }
    return new SampleNodeFeatsResult(results);
  }
}
