package com.tencent.angel.ml.GBDT.udf;

import com.tencent.angel.ml.tree.SplitEntry;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;


public class GBDTGradHistGetRowResult extends GetResult {

  private final SplitEntry splitEntry;

  public GBDTGradHistGetRowResult(ResponseType type, SplitEntry splitEntry) {
    super(type);
    this.splitEntry = splitEntry;
  }

  public SplitEntry getSplitEntry() {
    return splitEntry;
  }

}
