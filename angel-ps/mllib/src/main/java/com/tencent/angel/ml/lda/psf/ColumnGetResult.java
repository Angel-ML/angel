package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Map;

public class ColumnGetResult extends GetResult {

  public Map<Long, Int2IntOpenHashMap> cks;

  public ColumnGetResult(Map<Long, Int2IntOpenHashMap> cks) {
    this.cks = cks;
  }
}
