package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.IntKeyFeature;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.twitter.chill.ScalaKryoInstantiator;

import java.util.ArrayList;
import java.util.List;

public class SampleNodeFeat extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNodeFeat(SampleNodeFeatParam param) {
    super(param);
  }

  public SampleNodeFeat() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNodeFeatParam sampleParam = (PartSampleNodeFeatParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    int count = Math.min(row.size(), sampleParam.getCount());
    ByteArrayElement[] feats = SampleUtils.sampleNodeFeatByCount(row, count, System.currentTimeMillis());
    return new PartSampleNodeFeatResult(sampleParam.getPartKey().getPartitionId(), feats);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<Feature> results = new ArrayList<>();

    for (PartitionGetResult partResult : partResults) {
      PartSampleNodeFeatResult res = (PartSampleNodeFeatResult) partResult;
      if (res.getNodeFeats() != null) {
        for (ByteArrayElement feat: res.getNodeFeats()) {
          if (feat != null) {
            try {
              results.add(ScalaKryoInstantiator.defaultPool().fromBytes(feat.getData(), IntKeyFeature.class));
            } catch (Exception e) {
              System.err.println("Failed to deserialize feature data: " + e.getMessage());
            }

          }
        }
      }
    }
    return new SampleNodeFeatResult(results);
  }
}