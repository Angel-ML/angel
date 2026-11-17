package com.tencent.angel.graph.client.node2vec.getfuncs.getstats;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;

public class GetStats extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GetStats(GetStatsParam param) {
    super(param);
  }

  public GetStats() {
    super(null);
  }

  private int getBucketId(int key, int maxDegree, int minDegree, int numBuckets) {
    if (key == maxDegree) {
      return numBuckets - 1;
    } else {
      return (int) Math.floor(
          1.0 * numBuckets * Math.log(key - minDegree + 1) / Math.log(maxDegree - minDegree + 1));
    }
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetStatsPartitionParam pparam = (GetStatsPartitionParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(partParam.getPartKey(), 0);

    int maxDegree = pparam.getMaxDegree();
    int minDegree = pparam.getMinDegree();
    long numNode = 0;
    long numEdge = 0;
    int numBuckets = pparam.getNumBuckets();

    Int2IntOpenHashMap buckets = new Int2IntOpenHashMap();
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      LongArrayElement value = (LongArrayElement) entry.getValue();
      int degree = value.getData().length;
      numEdge += degree;

      int bucketId = getBucketId(degree, maxDegree, minDegree, numBuckets);

      if (buckets.containsKey(bucketId)) {
        buckets.addTo(bucketId, 1);
      } else {
        buckets.put(bucketId, 1);
      }
      numNode++;
    }

    return new GetStatsPartitionResult(numNode, numEdge, buckets);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2IntOpenHashMap buckets = new Int2IntOpenHashMap();
    long numNode = 0;
    long numEdge = 0;

    for (PartitionGetResult partResult : partResults) {
      Int2IntOpenHashMap partBuckets = ((GetStatsPartitionResult) partResult).getBuckets();
      numNode += ((GetStatsPartitionResult) partResult).getNumNode();
      numEdge += ((GetStatsPartitionResult) partResult).getNumEdge();

      ObjectIterator<Int2IntMap.Entry> iter = partBuckets.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        int key = entry.getIntKey();
        int value = entry.getIntValue();

        if (buckets.containsKey(key)) {
          buckets.addTo(key, value);
        } else {
          buckets.put(key, value);
        }
      }
    }

    return new GetStatsResult(numNode, numEdge, buckets);
  }
}
