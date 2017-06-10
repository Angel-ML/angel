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

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * The parameter server partitioner, controls the partitioning of the matrix which assigned to parameter servers.
 * The matrix split by block {@link MatrixContext#maxRowNumInBlock} and {@link MatrixContext#maxColNumInBlock},
 * and partitions block of matrix to related parameter server by {@link #assignPartToServer}.
 */
public class PSPartitioner implements Partitioner{
  private static final Log LOG = LogFactory.getLog(PSPartitioner.class);
  protected MatrixContext mContext;
  protected Configuration conf;

  @Override
  public void init(MatrixContext mtx, Configuration conf) {
    this.mContext = mtx;
    this.conf = conf;
  }

  @Override
  public List<Partition> getPartitions() {
    List<Partition> array = new ArrayList<Partition>();
    int id = 0;
    int matrixId = mContext.getId();
    int row = mContext.getRowNum();
    int col = mContext.getColNum();

    int blockRow = mContext.getMaxRowNumInBlock();
    int blockCol = mContext.getMaxColNumInBlock();
    if(blockRow == -1 || blockCol == -1) {
      int serverNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
      if(row >= serverNum) {
        blockRow = Math.min(row / serverNum, Math.max(1, 5000000 / col));
        blockCol = Math.min(5000000 / blockRow, col);
      } else {
        blockRow = row;
        blockCol = Math.min(5000000 / blockRow, Math.max(100, col / serverNum));
      }
    }

    LOG.info("blockRow = " + blockRow + ", blockCol=" + blockCol);

    Partition.Builder partition = Partition.newBuilder();
    for (int i = 0; i < row; i += blockRow) {
      for (int j = 0; j < col; j += blockCol) {
        int startRow = i;
        int startCol = j;
        int endRow = Math.min(i + blockRow, row);
        int endCol = Math.min(j + blockCol, col);
        partition.setMatrixId(matrixId);
        partition.setPartitionId(id++);
        partition.setStartRow(startRow);
        partition.setStartCol(startCol);
        partition.setEndRow(endRow);
        partition.setEndCol(endCol);
        array.add(partition.build());
      }
    }
    LOG.debug("partition count: " + array.size());
    return array;
  }

  @Override
  public int assignPartToServer(int partId) {
    int serverNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
    return partId % serverNum;
  }

  protected MatrixContext getMatrixContext() {
    return mContext;
  }
}
