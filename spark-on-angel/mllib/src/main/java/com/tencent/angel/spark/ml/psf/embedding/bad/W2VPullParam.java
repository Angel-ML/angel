package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;

import java.util.List;

public class W2VPullParam extends GetParam {

  int[] indices;
  int numNodePerRow;

  public W2VPullParam(int matrixId, int[] indices, int numNodePerRow) {
    super(matrixId);
    this.indices = indices;
    this.numNodePerRow = numNodePerRow;
  }

  @Override
  public List<PartitionGetParam> split() {




    return super.split();
  }
}
