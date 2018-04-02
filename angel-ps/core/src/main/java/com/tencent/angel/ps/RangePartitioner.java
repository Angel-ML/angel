/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of range partitioner
 */
public abstract class RangePartitioner implements Partitioner {
  private static final Log LOG = LogFactory.getLog(RangePartitioner.class);
  /**
   * Matrix context
   */
  protected MatrixContext mContext;

  /**
   * Application configuration
   */
  protected Configuration conf;
  protected final static long DEFAULT_PARTITION_SIZE = 500000;
  protected final static int maxPartNum = 10000;

  @Override public void init(MatrixContext mContext, Configuration conf) {
    this.mContext = mContext;
    this.conf = conf;
  }

  @Override public List<PartitionMeta> getPartitions() {
    List<PartitionMeta> partitions = new ArrayList<PartitionMeta>();
    int id = 0;
    int matrixId = mContext.getMatrixId();
    int row = mContext.getRowNum();
    long col = mContext.getColNum();
    long validIndexNum = mContext.getValidIndexNum();
    if(col > 0 && validIndexNum > col) {
      validIndexNum = col;
    }
    int blockRow = mContext.getMaxRowNumInBlock();
    long blockCol = mContext.getMaxColNumInBlock();
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    LOG.info("start to split matrix " + mContext);

    if(col == -1) {
      long partSize = DEFAULT_PARTITION_SIZE;
      if(validIndexNum > 0) {
        partSize = (long)(DEFAULT_PARTITION_SIZE * (row * (double)getMaxIndex() * 2 / validIndexNum));
      }
      if(blockRow == -1) {
        if (row > serverNum) {
          blockRow = (int) Math.min(row / serverNum, Math.max(row / maxPartNum, Math.max(1, partSize / ((double) getMaxIndex() * 2))));
        } else {
          blockRow = row;
        }
      }
      blockCol = Math.min(Math.max(100, (long)((double)getMaxIndex() * 2 / serverNum)),
        Math.max(partSize / blockRow, (long)(row * ((double)getMaxIndex() * 2 / maxPartNum / blockRow))));
    } else {
      if(blockRow == -1 || blockCol == -1) {
        long partSize = DEFAULT_PARTITION_SIZE;
        if(validIndexNum > 0) {
          partSize = (long)(DEFAULT_PARTITION_SIZE * (row * (double)col / validIndexNum));
        }
        if(blockRow == -1) {
          if(row > serverNum) {
            blockRow = (int) Math.min(row / serverNum, Math.max(row / maxPartNum, Math.max(1, partSize / col)));
          } else {
            blockRow = row;
          }
        }
        blockCol = Math.min(Math.max(100, col / serverNum), Math.max(partSize / blockRow, (long)(row * ((double)col / maxPartNum / blockRow))));
      }
    }

    LOG.info("blockRow = " + blockRow + ", blockCol=" + blockCol);

    long minValue = 0;
    long maxValue = 0;
    if(col == -1) {
      minValue = getMinIndex();
      maxValue = getMaxIndex();
    } else {
      minValue = 0;
      maxValue = col;
    }

    int startRow;
    int endRow;
    long startCol;
    long endCol;
    for (int i = 0; i < row; ) {
      for (long j = minValue; j < maxValue; ) {
        startRow = i;
        startCol = j;
        endRow = (i <= (row - blockRow)) ? (i + blockRow) : row;
        endCol = (j <= (maxValue - blockCol)) ? (j + blockCol) : maxValue;
        partitions.add(new PartitionMeta(matrixId, id++, startRow, endRow, startCol, endCol));
        j = (j <= (maxValue - blockCol)) ? (j + blockCol) : maxValue;
      }
      i = (i <= (row - blockRow)) ? (i + blockRow) : row;
    }

    LOG.info("partition count: " + partitions.size());
    return partitions;
  }

  @Override public int assignPartToServer(int partId) {
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    return partId % serverNum;
  }

  /**
   * Get max value of range
   * @return max value of range
   */
  protected abstract long getMaxIndex();

  /**
   * Get min value of range
   * @return min value of range
   */
  protected abstract long getMinIndex();
}
