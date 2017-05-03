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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixPartitionLocation;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSIdProto;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.MLProtos.Partition;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * The parameter server partitioner, controls the partitioning of the matrix which assigned to parameter servers.
 * The matrix split by block {@link MatrixContext#maxRowNumInBlock} and {@link MatrixContext#maxColNumInBlock},
 * and partitions block of matrix to related parameter server by {@link #getServerIndex}.
 */
public class PSPartitioner {
  private static final Log LOG = LogFactory.getLog(PSPartitioner.class);
  private static int matrixGenerateIndex = 1;
  protected MatrixContext mContext;
  protected Configuration conf;
  protected int matrixId;


  /**
   * Sets.
   *
   * @param mtx  the matrix context
   * @param conf the conf
   */
  public void setup(MatrixContext mtx, Configuration conf) {
    this.mContext = mtx;
    this.conf = conf;
  }

  /**
   * Gets server index by partition.
   *
   * @param partition the partition
   * @param numServer the num server
   * @return the server index
   */
  public int getServerIndex(Partition partition, int numServer) {
    return partition.getPartitionId() % numServer;
  }


  /**
   * Generate matrix proto.
   *
   * @return the matrix proto
   */
  public final MatrixProto generateMatrixProto() {
    MatrixProto.Builder mProtoBuilder = MatrixProto.newBuilder();
    mProtoBuilder.setName(mContext.getName());
    matrixId = generateMatrixId();
    mProtoBuilder.setId(matrixId);
    mProtoBuilder.setRowNum(mContext.getRowNum());
    mProtoBuilder.setColNum(mContext.getColNum());
    mProtoBuilder.setRowType(mContext.getRowType());
    // set MatrixPartitionLocation
    MatrixPartitionLocation.Builder mpLocBuild = MatrixPartitionLocation.newBuilder();
    for (Partition part : getPartitions()) {
      mpLocBuild.setPart(part);
      mpLocBuild.setPsId(PSIdProto.newBuilder().setPsIndex(getServerIndex(part, conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, 1))).build());
      mProtoBuilder.addMatrixPartLocation(mpLocBuild.build());
    }
    LOG.info("partition num: " + mProtoBuilder.getMatrixPartLocationCount());
    // set attribute
    Pair.Builder attrBuilder = Pair.newBuilder();
    for (Map.Entry<String, String> entry : mContext.getAttributes().entrySet()) {
      attrBuilder.setKey(entry.getKey());
      attrBuilder.setValue(entry.getValue());
      mProtoBuilder.addAttribute(attrBuilder.build());
    }
    return mProtoBuilder.build();
  }

  /**
   * Gets partitions of matrix
   *
   * @return the partitions
   */
  public List<Partition> getPartitions() {
    List<Partition> array = new ArrayList<Partition>();
    int id = 0;
    int row = mContext.getRowNum();
    int col = mContext.getColNum();

    int blockRow = mContext.getMaxRowNumInBlock();
    int blockCol = mContext.getMaxColNumInBlock();
    if(blockRow == -1 || blockCol == -1) {
      int serverNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
      if(row >= serverNum) {
        blockRow = row / serverNum;
      } else {
        blockRow = row;
      }
      
      if(col >= serverNum) {
        blockCol = Math.min(5000000, col / serverNum);
      } else {
        blockCol = col;
      }     
    }
    
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

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * generate an unique id for matrix.
   *
   * @return the int
   */
  public static final int generateMatrixId() {
    return matrixGenerateIndex++;
  }

  protected MatrixContext getMatrixContext() {
    return mContext;
  }
}
