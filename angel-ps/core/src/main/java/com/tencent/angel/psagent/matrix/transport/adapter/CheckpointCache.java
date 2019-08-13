package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;

public class CheckpointCache extends PartitionResponseCache<VoidResult> {

  public CheckpointCache(int totalRequestNum) {
    super(totalRequestNum);
  }
}
