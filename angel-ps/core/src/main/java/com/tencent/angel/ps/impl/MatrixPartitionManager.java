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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos;
import com.tencent.angel.ps.impl.matrix.RowUpdater;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.utils.StringUtils;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixPartition;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;

/**
 * The matrix partitions manager on the parameter server.
 */
public class MatrixPartitionManager {
  private final static Log LOG = LogFactory.getLog(MatrixPartitionManager.class);
  /** matrixId->Matrix  */
  private final ConcurrentHashMap<Integer, ServerMatrix> matrixIdMap;

  /** Matrix row updater */
  private volatile RowUpdater rowUpdater;

  /**
   * Create a new Matrix partition manager.
   */
  public MatrixPartitionManager() {
    matrixIdMap = new ConcurrentHashMap<Integer, ServerMatrix>();
  }

  public void init() throws IllegalAccessException, InstantiationException {
    Configuration conf = PSContext.get().getConf();
    Class<?> rowUpdaterClass =
      conf.getClass(AngelConf.ANGEL_PS_ROW_UPDATER_CLASS,
        AngelConf.DEFAULT_ANGEL_PS_ROW_UPDATER);
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
  public void writeSnapshot(DataOutputStream output) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("matrixMap size: " + matrixIdMap.size());
    }
    output.writeInt(matrixIdMap.size());
    for (Map.Entry<Integer, ServerMatrix> entry : matrixIdMap.entrySet()) {
      output.writeInt(entry.getKey());
      entry.getValue().writeSnapshot(output);
    }
  }

  /**
   * Parse matrices from input.
   *
   * @param input the input
   * @throws IOException the io exception
   */
  public void readSnapshot(DataInputStream input) throws IOException {
    int size = input.readInt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("read size from input: " + size);
    }
    for (int i = 0; i < size; ++i) {
      int matrixId = input.readInt();
      matrixIdMap.get(matrixId).readSnapshot(input);
    }
  }

  /**
   * Add matrix partitions to parameter server.
   *
   * @param matrixPartitions the matrix partitions
   * @throws IOException load matrix partition from files failed
   */
  public void addMatrixPartitions(List<PSMasterServiceProtos.MatrixPartition> matrixPartitions) throws IOException {
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
    serverMatrix.load();
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
   * @param matrixId matrix id
   * @return ServerMatrix matrix
  */      
  public ServerMatrix getMatrix(int matrixId) {
    return matrixIdMap.get(matrixId);
  }

  public void commit(List<Integer> matrixIds) throws IOException {
    if(matrixIds == null || matrixIds.isEmpty()) {
      LOG.info("there are no matrices committed");
      return;
    }
    Configuration conf = PSContext.get().getConf();
    String outputPath = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    LOG.info("start to write matrices :" + getMatrixNames(matrixIds) + " to " + outputPath);

    Path baseDir = new Path(new Path(outputPath, ModelFilesConstent.resultDirName), PSContext.get().getPs().getServerId().toString());
    FileSystem fs = baseDir.getFileSystem(conf);
    if (fs.exists(baseDir)) {
      LOG.warn("ps temp output directory " + baseDir.toString() + " is already existed , just remove it");
      fs.delete(baseDir, true);
    }

    Vector<String> errorLogs = new Vector<>();
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(fs, baseDir, ACTION.COMMIT, errorLogs, matrixIds, 0, matrixIds.size());
      MatrixDiskIOExecutors.execute(commitOp);
      commitOp.join();
      if(!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    }

    return;
  }

  private void commitMatrix(FileSystem fs, Path outputPath, int matrixId) throws IOException {
    ServerMatrix matrix = matrixIdMap.get(matrixId);
    if(matrix != null) {
      matrix.commit(fs, outputPath);
    }
  }

  private String getMatrixNames(List<Integer> matrixIds) {
    int size = matrixIds.size();
    StringBuilder sb = new StringBuilder();
    for(int i = 0; i < size; i++) {
      ServerMatrix matrix = getMatrix(matrixIds.get(i));
      if(matrix != null) {
        sb.append(matrix.getName());
      } else {
        sb.append("null");
      }

      if(i < size - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }

  enum ACTION {
    LOAD, COMMIT
  }

  class MatrixDiskIOOp extends RecursiveAction {
    private final FileSystem fs;
    private final Path outputPath;
    private final ACTION action;
    private final Vector<String> errorLogs;
    private final List<Integer> matrixIds;
    private final int startPos;
    private final int endPos;

    public MatrixDiskIOOp(FileSystem fs, Path outputPath, ACTION action, Vector<String> errorLogs, List<Integer> matrixIds, int start, int end) {
      this.fs = fs;
      this.outputPath = outputPath;
      this.action = action;
      this.errorLogs = errorLogs;
      this.matrixIds = matrixIds;
      this.startPos = start;
      this.endPos = end;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          commitMatrix(fs, outputPath, matrixIds.get(startPos) );
        } catch (IOException e) {
          String errorLog = "commit matrix " + matrixIdMap.get(matrixIds.get(startPos)).getMatrixName() + " failed " + e.getMessage();
          LOG.error(errorLog);
          errorLogs.add(errorLog);
        }
      } else {
        int middle = (startPos + endPos) / 2;
        MatrixDiskIOOp opLeft = new MatrixDiskIOOp(fs, outputPath, action, errorLogs, matrixIds, startPos, middle);
        MatrixDiskIOOp opRight = new MatrixDiskIOOp(fs, outputPath, action, errorLogs, matrixIds, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }
}
