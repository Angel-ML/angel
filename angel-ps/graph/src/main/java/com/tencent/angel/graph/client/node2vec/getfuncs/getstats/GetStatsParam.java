package com.tencent.angel.graph.client.node2vec.getfuncs.getstats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class GetStatsParam extends GetParam {

  private int maxDegree;
  private int minDegree;
  private int numBuckets;

  public GetStatsParam(int matrixId, int maxDegree, int minDegree, int numBuckets) {
    super(matrixId);
    this.maxDegree = maxDegree;
    this.minDegree = minDegree;
    this.numBuckets = numBuckets;
  }

  public GetStatsParam() {
    super();
  }

  public int getMaxDegree() {
    return maxDegree;
  }

  public void setMaxDegree(int maxDegree) {
    this.maxDegree = maxDegree;
  }

  public int getMinDegree() {
    return minDegree;
  }

  public void setMinDegree(int minDegree) {
    this.minDegree = minDegree;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : parts) {
      partParams.add(new GetStatsPartitionParam(matrixId, part, maxDegree, minDegree, numBuckets));
    }

    return partParams;
  }
}
