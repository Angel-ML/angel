package com.tencent.angel.master;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ps.IntRangePartitioner;
import com.tencent.angel.ps.LongRangePartitioner;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class PartitionerTest {
  @Test
  public void testModelLongPartitioner() throws Exception {
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000000);
    //mMatrix.setNnz(100000000);
    Configuration conf = new Configuration();
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    LongRangePartitioner partitioner = new LongRangePartitioner();
    partitioner.init(mMatrix, conf);
    partitioner.getPartitions();
  }

  @Test
  public void testModelIntPartitioner() throws Exception {
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w2");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(400000000);
    //mMatrix.setNnz(100000000);
    Configuration conf = new Configuration();
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    IntRangePartitioner partitioner = new IntRangePartitioner();
    partitioner.init(mMatrix, conf);
    partitioner.getPartitions();
  }
}
