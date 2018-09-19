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
import com.tencent.angel.ml.matrix.PartitionMeta;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

public class ColumnRangePartitioner extends RangePartitioner {
  private static final Log LOG = LogFactory.getLog(ColumnRangePartitioner.class);
  @Override public List<PartitionMeta> getPartitions() {
    List<PartitionMeta> partitions = new ArrayList<>();

    int id = 0;
    int matrixId = mContext.getMatrixId();
    int row = mContext.getRowNum();
    long col = mContext.getColNum();
    long validIndexNum = mContext.getValidIndexNum();
    if (col > 0 && validIndexNum > col)
      validIndexNum = col;

    int blockRow = mContext.getMaxRowNumInBlock();
    long blockCol = mContext.getMaxColNumInBlock();

    int serverNum = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);

    if (blockRow == -1 || blockCol == -1) {
      long partSize = DEFAULT_PARTITION_SIZE;
      if (validIndexNum > 0) {
        partSize = (long) (DEFAULT_PARTITION_SIZE * (row * (double) col / validIndexNum));
      }
      blockRow = row;
      blockCol = Math.min(Math.max(100, col / serverNum),
        Math.max(partSize / blockRow, (long) (row * ((double) col / maxPartNum / blockRow))));
    }

    mContext.setMaxRowNumInBlock(blockRow);
    mContext.setMaxColNumInBlock(blockCol);

    long minValue = 0;
    long maxValue = col;

    LOG.info("blockRow=" + blockRow + ", blockCol=" + blockCol);

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

    LOG.info("partitions number is " + partitions.size());
    return partitions;
  }

  @Override protected long getMaxIndex() {
    return 0;
  }

  @Override protected long getMinIndex() {
    return 0;
  }
}
