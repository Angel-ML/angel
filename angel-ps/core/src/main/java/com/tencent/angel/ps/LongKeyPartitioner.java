package com.tencent.angel.ps;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

public class LongKeyPartitioner implements Partitioner {
  private static final Log LOG = LogFactory.getLog(LongKeyPartitioner.class);
  protected MatrixContext mContext;
  protected Configuration conf;
  private final int partNumPerPS;
  private final static int defaultPartNumPerPS = 10;

  public LongKeyPartitioner() {
    this(defaultPartNumPerPS);
  }

  public LongKeyPartitioner(int partNumPerPS) {
    this.partNumPerPS = partNumPerPS;
  }

  @Override public void init(MatrixContext mContext, Configuration conf) {
    this.mContext = mContext;
    this.conf = conf;
  }

  @Override public List<MLProtos.Partition> getPartitions() {
    List<MLProtos.Partition> array = new ArrayList<MLProtos.Partition>();
    int id = 0;
    int matrixId = mContext.getId();
    int row = mContext.getRowNum();
    long col = mContext.getColNum();

    int blockRow = mContext.getMaxRowNumInBlock();
    long blockCol = mContext.getMaxColNumInBlock();
    if(blockRow == -1 || blockCol == -1) {
      blockRow = 1;
      int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
      blockCol = Long.MAX_VALUE / serverNum / partNumPerPS;
    }

    LOG.info("blockRow = " + blockRow + ", blockCol=" + blockCol);

    MLProtos.Partition.Builder partition = MLProtos.Partition.newBuilder();
    if(col == -1) {
      col = Long.MAX_VALUE;
      for (int i = 0; i < row; ) {
        for (long j = Long.MIN_VALUE; j < col; ) {
          int startRow = i;
          long startCol = j;
          int endRow = (i <= (row - blockRow)) ? (i + blockRow) : row;
          long endCol = (j <= (col - blockCol)) ? (j + blockCol) : col;
          partition.setMatrixId(matrixId);
          partition.setPartitionId(id++);
          partition.setStartRow(startRow);
          partition.setStartCol(startCol);
          partition.setEndRow(endRow);
          partition.setEndCol(endCol);
          array.add(partition.build());

          j = (j <= (col - blockCol)) ? (j + blockCol) : col;
        }
        i = (i <= (row - blockRow)) ? (i + blockRow) : row;
      }
    } else {
      for (int i = 0; i < row; ) {
        for (long j = 0; j < col; ) {
          int startRow = i;
          long startCol = j;
          int endRow = (i <= (row - blockRow)) ? (i + blockRow) : row;
          long endCol = (j <= (col - blockCol)) ? (j + blockCol) : col;
          partition.setMatrixId(matrixId);
          partition.setPartitionId(id++);
          partition.setStartRow(startRow);
          partition.setStartCol(startCol);
          partition.setEndRow(endRow);
          partition.setEndCol(endCol);
          array.add(partition.build());

          j = (j <= (col - blockCol)) ? (j + blockCol) : col;
        }
        i = (i <= (row - blockRow)) ? (i + blockRow) : row;
      }
    }

    LOG.debug("partition count: " + array.size());
    return array;
  }

  @Override public int assignPartToServer(int partId) {
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    return partId % serverNum;
  }
}
