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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ps;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.PartitionMeta;
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
  private final static long DEFAULT_PARTITION_SIZE = 500000;

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

  @Override public List<PartitionMeta> getPartitions() {
    List<PartitionMeta> partitions = new ArrayList<PartitionMeta>();
    int id = 0;
    int matrixId = mContext.getMatrixId();
    int row = mContext.getRowNum();
    long col = mContext.getColNum();

    int blockRow = mContext.getMaxRowNumInBlock();
    long blockCol = mContext.getMaxColNumInBlock();
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    if(col == -1) {
      blockRow = 1;

      blockCol = Long.MAX_VALUE / serverNum / partNumPerPS * 2;
    } else {
      if(blockRow == -1 || blockCol == -1) {
        if(row >= serverNum) {
          blockRow = (int) Math.min(row / serverNum, Math.max(1, DEFAULT_PARTITION_SIZE / col));
          blockCol = Math.min(DEFAULT_PARTITION_SIZE / blockRow, col);
        } else {
          blockRow = row;
          blockCol = Math.min(DEFAULT_PARTITION_SIZE / blockRow, Math.max(100, col / serverNum));
        }
      }
    }

    LOG.info("blockRow = " + blockRow + ", blockCol=" + blockCol);

    long minValue = 0;
    long maxValue = 0;
    if(col == -1) {
      minValue = Long.MIN_VALUE;
      maxValue = Long.MAX_VALUE;
    } else {
      minValue = 0;
      maxValue = col;
    }

    int startRow = 0;
    int endRow = 0;
    long startCol = 0;
    long endCol = 0;
    for (int i = 0; i < row; ) {
      for (long j = minValue; j < maxValue; ) {
        startRow = i;
        startCol = j;
        endRow = (i <= (row - blockRow)) ? (i + blockRow) : row;
        endCol = (j <= (col - blockCol)) ? (j + blockCol) : col;
        partitions.add(new PartitionMeta(matrixId, id++, startRow, endRow, startCol, endCol));
        j = (j <= (col - blockCol)) ? (j + blockCol) : col;
      }
      i = (i <= (row - blockRow)) ? (i + blockRow) : row;
    }

    LOG.debug("partition count: " + partitions.size());
    return partitions;
  }

  @Override public int assignPartToServer(int partId) {
    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    return partId % serverNum;
  }
}
