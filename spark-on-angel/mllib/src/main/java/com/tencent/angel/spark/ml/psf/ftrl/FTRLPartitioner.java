package com.tencent.angel.spark.ml.psf.ftrl;

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ps.storage.partitioner.ColumnRangePartitioner;

import java.util.List;

public class FTRLPartitioner extends ColumnRangePartitioner {

  long start, end;

  @Override
  public List<PartitionMeta> getPartitions() {

    start = mContext.getMaxColNumInBlock();
    end = mContext.getColNum();

    mContext.setMaxRowNumInBlock(mContext.getRowNum());
    mContext.setMaxColNumInBlock(-1);
    return super.getPartitions();
  }

  @Override
  protected long getMaxIndex(MatrixContext mContext) {
    return end;
  }

  @Override
  protected long getMinIndex(MatrixContext mContext) {
    return start;
  }
}
