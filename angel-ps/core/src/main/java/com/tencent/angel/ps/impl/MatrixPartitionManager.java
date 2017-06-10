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

package com.tencent.angel.ps.impl;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ps.impl.matrix.RowUpdater;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixPartition;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The matrices manager on parameter server.
 *
 */
public class MatrixPartitionManager {
  private final static Log LOG = LogFactory.getLog(MatrixPartitionManager.class);
  private final ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap; // matrixId---->Matrix

  private final RowUpdater rowUpdater;

  /**
   * Create a new Matrix partition manager.
   *
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public MatrixPartitionManager() throws InstantiationException, IllegalAccessException {
    matrixIdMap = new ConcurrentHashMap<Integer, ServerMatrix>();
    Configuration conf = PSContext.get().getConf();
    Class<?> rowUpdaterClass =
        conf.getClass(AngelConfiguration.ANGEL_PS_ROW_UPDATER_CLASS,
            AngelConfiguration.DEFAULT_ANGEL_PS_ROW_UPDATER);
    rowUpdater = (RowUpdater) rowUpdaterClass.newInstance();
  }

  /**
   * Gets all matrices on parameter server
   *
   * @return the matrices
   */
  public ConcurrentHashMap<Integer, ServerMatrix> getMatrixIdMap() {
    return matrixIdMap;
  }

  /**
   * Write matrices to output
   *
   * @param output the output
   * @throws IOException
   */
  public void writeMatrix(DataOutputStream output) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("matrixMap size: " + matrixIdMap.size());
    }
    output.writeInt(matrixIdMap.size());
    for (Map.Entry<Integer, ServerMatrix> entry : matrixIdMap.entrySet()) {
      output.writeInt(entry.getKey());
      entry.getValue().writeTo(output);
    }
  }

  /**
   * Parse matrices from input.
   *
   * @param input the input
   * @throws IOException the io exception
   */
  public void parseMatricesFromInput(DataInputStream input) throws IOException {
    int size = input.readInt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("read size from input: " + size);
    }
    for (int i = 0; i < size; ++i) {
      int matrixId = input.readInt();
      matrixIdMap.get(matrixId).readFrom(input);
    }
  }

  /**
   * Add matrix partitions to parameter server.
   *
   * @param matrixPartitions the matrix partitions
   * @throws IOException load matrix partition from files failed
   */
  public void addMatrixPartitions(List<MatrixPartition> matrixPartitions) throws IOException {
    int size = matrixPartitions.size();
    for (int i = 0; i < size; i++) {
      LOG.info("add matrix partition " + matrixPartitions.get(i));
      addServerMatrix(matrixPartitions.get(i));
    }
  }


  /**
   * Remove matrices from parameter server.
   *
   * @param needReleaseMatricesList the release matrices list
   */
  public void removeMatrices(List<Integer> needReleaseMatricesList) {
    int size = needReleaseMatricesList.size();
    for (int i = 0; i < size; i++) {
      matrixIdMap.remove(needReleaseMatricesList.get(i));
    }
  }

  private void addServerMatrix(MatrixPartition matrixPartition) throws IOException {
    int matrixId = matrixPartition.getMatrixId();
    if (matrixIdMap.containsKey(matrixId)) {
      LOG.warn("MatrixId [" + matrixId + "] has already been added.");
      return;
    }
    ServerMatrix serverMatrix = new ServerMatrix(matrixPartition);
    serverMatrix.loadPartitions();
    LOG.info("MatrixId [" + matrixId + "] added.");
    matrixIdMap.putIfAbsent(matrixId, serverMatrix);
  }

  public ServerRow getRow(PartitionKey partitionKey, int rowId) {
    ServerMatrix matrix = matrixIdMap.get(partitionKey.getMatrixId());
    if (matrix != null) {
      LOG.debug("before get row, partition key is " + partitionKey);
      return matrix.getPartition(partitionKey).getRow(rowId);
    } else {
      return null;
    }
  }

  /**
   * Gets server partition of matrix by partition key
   *
   * @param partitionKey the partition key
   * @return the server partition if exists,else null
   */
  public ServerPartition getServerPartition(PartitionKey partitionKey) {
    ServerMatrix matrix = matrixIdMap.get(partitionKey.getMatrixId());
    if (matrix != null) {
      return matrix.getPartition(partitionKey);
    } else {
      return null;
    }
  }

  /**
   * Update related rows of partition which specified by partition key
   *
   * @param partitionKey the partition key
   * @param buf          the data buf
   * @throws Exception
   */
  public void update(PartitionKey partitionKey, ByteBuf buf) throws Exception {
    int rowNum = buf.readInt();
    ServerMatrix matrix = matrixIdMap.get(partitionKey.getMatrixId());

    if (matrix != null) {
      int rowId;
      MLProtos.RowType rowType;
      int size;

      for (int i = 0; i < rowNum; i++) {
        rowId = buf.readInt();
        rowType = MLProtos.RowType.valueOf(buf.readInt());
        size = buf.readInt();

        if (size == 0)
          continue;

        LOG.debug("rowId = " + rowId + " rowType = " + rowType + " size = " + size + " request " +
            "update");

        ServerRow row = matrix.getPartition(partitionKey).getRow(rowId);
        rowUpdater.update(rowType, size, buf, row);
      }
    }
  }

  /**
   * Clock specified task of partition
   *
   * @param partitionKey the partition key
   * @param taskIndex    the task index
   * @param clock        the clock
   */
  public void clock(PartitionKey partitionKey, int taskIndex, int clock) {
    ServerMatrix matrix = matrixIdMap.get(partitionKey.getMatrixId());
    if (matrix != null) {
      ServerPartition part = matrix.getPartition(partitionKey);
      if (part != null) {
        part.clock(taskIndex, clock);
      }
    }
  }

  /**
   * Clear matrices on parameter server
   */
  public void clear() {
    matrixIdMap.clear();
  }

  /**
   * Gets related row.
   *
   * @param matrixId the matrix id
   * @param rowId    the row id
   * @param partId   the partition id
   * @return the row if exists, else null
   */
  public ServerRow getRow(int matrixId, int rowId, int partId) {
    ServerMatrix matrix = matrixIdMap.get(matrixId);
    if (matrix != null) {
      return matrix.getPartition(partId).getRow(rowId);
    } else {
      return null;
    }
  }

  /**
   * Partition ready, compare partition's clock with specified clock.
   *
   * @param partKey the partition key
   * @param clock   the clock
   * @return true if partition's clock greater or equals specified clock,else false
   */
  public boolean partitionReady(PartitionKey partKey, int clock) {
    ServerPartition partition = getPartition(partKey.getMatrixId(), partKey.getPartitionId());
    return partition != null && partition.getClock() >= clock;
  }

  /**
   * Gets related partition.
   *
   * @param matrixId the matrix id
   * @param partId   the part id
   * @return the partition
   */
  public ServerPartition getPartition(int matrixId, int partId) {
    ServerMatrix matrix = matrixIdMap.get(matrixId);
    if (matrix != null) {
      return matrix.getPartition(partId);
    } else {
      return null;
    }
  }

  /**
   * Gets clocks for all matrices
   *
   * @param clocks the clocks
   */
  public void getClocks(Object2IntOpenHashMap<PartitionKey> clocks) {
    for (Entry<Integer, ServerMatrix> matrixEntry : matrixIdMap.entrySet()) {
      matrixEntry.getValue().getClocks(clocks);
    }
  }

  /**
   * Sets clock of task.
   *
   * @param matrixId  the matrix id
   * @param taskIndex the task index
   * @param clock     the clock
   */
  public void setClock(int matrixId, int taskIndex, int clock) {
    ServerMatrix matrix = matrixIdMap.get(matrixId);
    if(matrix == null) {
      return;
    }
    
    matrix.setClock(taskIndex, clock);
  }

  /**
   * Get matrix use matrix id
   * 
   * @param integer matrix id
   * @return ServerMatrix matrix
  */      
  public ServerMatrix getMatrix(int matrixId) {
    return matrixIdMap.get(matrixId);
  }
}
