/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ps.storage.partitioner;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of range partitioner
 */
public class RangePartitioner implements Partitioner {

  private static final Log LOG = LogFactory.getLog(RangePartitioner.class);
  /**
   * Matrix context
   */
  protected MatrixContext mContext;

  /**
   * Application configuration
   */
  protected Configuration conf;
  protected long DEFAULT_PARTITION_SIZE;
  protected int maxPartNum;

  @Override
  public void init(MatrixContext mContext, Configuration conf) {
    this.mContext = mContext;
    this.conf = conf;

    long defaultPartSize = conf.getLong(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_SIZE,
            AngelConf.DEFAULT_ANGEL_MODEL_PARTITIONER_PARTITION_SIZE);
    int maxPartNumTotal = conf.getInt(AngelConf.ANGEL_MODEL_PARTITIONER_MAX_PARTITION_NUM,
            AngelConf.DEFAULT_ANGEL_MODEL_PARTITIONER_MAX_PARTITION_NUM);
    int psNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    int partNumPerServer =
            conf.getInt(AngelConf.ANGEL_MODEL_PARTITIONER_PARTITION_NUM_PERSERVER, -1);

    if (partNumPerServer > 0) {
      maxPartNum = Math.min(maxPartNumTotal, psNum * partNumPerServer);
    } else {
      maxPartNum = maxPartNumTotal;
    }
    DEFAULT_PARTITION_SIZE = defaultPartSize;
  }

  @Override
  public List<PartitionMeta> getPartitions() {
    List<PartitionMeta> partitions = new ArrayList<PartitionMeta>();
    int id = 0;
    int matrixId = mContext.getMatrixId();
    int row = mContext.getRowNum();
    long col = mContext.getColNum();
    long start = mContext.getIndexStart();
    long end = mContext.getIndexEnd();

    long validIndexNum = mContext.getValidIndexNum();

    if (col > 0 && validIndexNum > col)
      validIndexNum = col;

    int blockRow = mContext.getMaxRowNumInBlock();
    long blockCol = mContext.getMaxColNumInBlock();
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    LOG.info("start to split matrix " + mContext);

    double range = col;
    // if col == -1, we use the start/end index to calculate range,
    // we use double to store the range value since two long minus might exceed the
    // range of long.
    if (col == -1)
      range = ((double) end - (double) start);

    long partSize = DEFAULT_PARTITION_SIZE;

    if (validIndexNum > 0)
      partSize = (long) (DEFAULT_PARTITION_SIZE * (range / validIndexNum));

    if (blockRow < 0) {
      if (row > serverNum)
        blockRow = (int) Math.min(row / serverNum,
                Math.max(row / maxPartNum, Math.max(1, partSize / range)));
      else
        blockRow = row;
    }

    if (blockCol < 0)
      blockCol = Math.min(Math.max(100, col / serverNum),
              Math.max(partSize / blockRow, (long) (row * (range / maxPartNum / blockRow))));

    LOG.info("blockRow = " + blockRow + ", blockCol=" + blockCol);
    mContext.setMaxRowNumInBlock(blockRow);
    mContext.setMaxColNumInBlock(blockCol);

    int startRow;
    int endRow;
    long startCol;
    long endCol;
    for (int i = 0; i < row; ) {
      for (long j = start; j < end; ) {
        startRow = i;
        startCol = j;
        endRow = (i <= (row - blockRow)) ? (i + blockRow) : row;
        endCol = (j <= (end - blockCol)) ? (j + blockCol) : end;
        partitions.add(new PartitionMeta(matrixId, id++, startRow, endRow, startCol, endCol));
        j = (j <= (end - blockCol)) ? (j + blockCol) : end;
      }
      i = (i <= (row - blockRow)) ? (i + blockRow) : row;
    }

    LOG.info("partition count: " + partitions.size());
    return partitions;
  }


  @Override
  public int assignPartToServer(int partId) {
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    return partId % serverNum;
  }
}
