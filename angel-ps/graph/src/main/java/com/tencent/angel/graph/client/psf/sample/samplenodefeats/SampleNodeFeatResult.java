package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import java.util.Collections;
import java.util.List;

public class SampleNodeFeatResult extends GetResult {

  /**
   * randomly sampled node features
   */
  public List<Feature> feats;

  public SampleNodeFeatResult(List<Feature> feats) {
    this.feats = feats;
  }

  SampleNodeFeatResult() {
    this(null);
  }

  /**
   * get node feats when format is dense or sparse
   *
   * @return feats1
   */
  public Feature[] getNodeFeats() {
    Collections.shuffle(feats);
    Feature[] res = new Feature[feats.size()];
    return feats.toArray(res);
  }
}