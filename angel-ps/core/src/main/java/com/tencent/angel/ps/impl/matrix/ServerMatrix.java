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

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.math.VectorType;
import com.tencent.angel.ml.math.vector.CompDoubleLongKeyVector;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.MLProtos.Partition;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixPartition;
import com.tencent.angel.ps.impl.MatrixDiskIOExecutors;
import com.tencent.angel.ps.impl.PSContext;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * The Server matrix on parameter server,assigned by {@link com.tencent.angel.master.AngelApplicationMaster},which represents a set of partitions of matrix
 *
 * @see com.tencent.angel.ml.matrix.MatrixMeta
 * @see com.tencent.angel.master.MatrixMetaManager
 */
public class ServerMatrix {

  private final static Log LOG = LogFactory.getLog(ServerMatrix.class);

  /**
   * Mapping from matrix PartitionKey.partitionId to ServerPartition.
   */
  private HashMap<Integer, ServerPartition> partitionMaps;

  private final Map<String, String> attribute;

  private int matrixId;

  private String matrixName;

  private List<PartitionKey> partitionKeys;

  /**
   * Create a new Server matrix by matrix partition.
   *
   * @param matrix the matrix partition contains a set of partitions, which need to load on Parameter Server
   */
  public ServerMatrix(MatrixPartition matrix) {
    this.matrixId = matrix.getMatrixId();
    this.matrixName = matrix.getMatrixName();

    LOG.info("Creating a Server Matrix, id: " + matrixId + ", name: " + matrixName);
    partitionKeys = new ArrayList<>(matrix.getPartitionsCount());
    attribute = new HashMap<>();
    for (Pair pair : matrix.getConfigurationsList()) {
      attribute.put(pair.getKey(), pair.getValue());
    }
    partitionMaps = new HashMap<>();

    for (Partition partition : matrix.getPartitionsList()) {
      PartitionKey partitionKey = ProtobufUtil.convertPartition(partition);
      MLProtos.RowType rowType = matrix.getRowType();
      partitionKeys.add(partitionKey);
      partitionMaps.put(partitionKey.getPartitionId(), new ServerPartition(partitionKey, rowType));
    }
  }

  /**
   * Load matrix partitions from files.
   *
   * @throws IOException read files error
   */
  public void loadPartitions() throws IOException {
    String path = attribute.get(MatrixConf.MATRIX_LOAD_PATH);
    if (path == null) {
      return;
    }

    Configuration conf = PSContext.get().getConf();
    Path matrixPath = new Path(path, matrixName);
    FileSystem fs = matrixPath.getFileSystem(conf);
    List<Map.Entry<Integer, ServerPartition>> partitions =
      new ArrayList<>(partitionMaps.entrySet());
    Vector<String> errorMsgs = new Vector<>();
    PartitionLoader loader =
      new PartitionLoader(matrixPath, fs, partitions, errorMsgs, 0, partitions.size());
    MatrixDiskIOExecutors.execute(loader);

    if (errorMsgs.size() > 0) {
      throw new IOException(
        "load matrix " + matrixName + " partitions failed:" + toString(errorMsgs));
    }
  }

  private String toString(Vector<String> errorMsgs) {
    if (errorMsgs == null || errorMsgs.isEmpty()) {
      return "";
    }

    int size = errorMsgs.size();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      sb.append(errorMsgs.get(i));
      sb.append("\n");
    }
    return sb.toString();
  }

  class PartitionLoader extends RecursiveAction {
    private final Path matrixPath;
    private final FileSystem fs;
    private final List<Map.Entry<Integer, ServerPartition>> partitions;
    private final Vector<String> errorMsgs;
    private final int startPos;
    private final int endPos;

    public PartitionLoader(Path matrixPath, FileSystem fs,
      List<Map.Entry<Integer, ServerPartition>> partitions, Vector<String> errorMsgs, int startPos,
      int endPos) {
      this.matrixPath = matrixPath;
      this.fs = fs;
      this.partitions = partitions;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        Map.Entry<Integer, ServerPartition> partitionEntry = partitions.get(startPos);
        if (partitionEntry == null) {
          return;
        } else {
          try {
            loadPartition(matrixPath, fs, partitionEntry.getKey(), partitionEntry.getValue());
          } catch (Throwable x) {
            LOG.error("load partition " + partitionEntry + " failed.", x);
            errorMsgs.add("load partition " + partitionEntry + " failed." + x.getMessage());
          }
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PartitionLoader opLeft =
          new PartitionLoader(matrixPath, fs, partitions, errorMsgs, startPos, middle);
        PartitionLoader opRight =
          new PartitionLoader(matrixPath, fs, partitions, errorMsgs, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void loadPartition(Path matrixPath, FileSystem fs, int partId, ServerPartition partition)
    throws IOException {
    LOG.info("Load partition " + partId + " from path " + matrixPath);
    Path partitionFilePath = new Path(matrixPath, String.valueOf(partId));
    FSDataInputStream input = fs.open(partitionFilePath);

    // Pass the matrix and partition number field
    input.readInt();
    input.readInt();
    partition.load(input);
    input.close();
  }

  /**
   * Gets partition specified by partition key.
   *
   * @param partitionKey the partition key
   * @return the partition
   */
  public ServerPartition getPartition(PartitionKey partitionKey) {
    return partitionMaps.get(partitionKey.getPartitionId());
  }

  /**
   * Gets total partition keys.
   *
   * @return the total partition keys
   */
  public List<PartitionKey> getTotalPartitionKeys() {
    return partitionKeys;
  }

  /**
   * Gets the matrix name.
   *
   * @return the name
   */
  public String getName() {
    return matrixName;
  }


  /**
   * Gets the matrix id.
   *
   * @return the id
   */
  public int getId() {
    return matrixId;
  }

  /**
   * Read partitions of matrix from input
   *
   * @param input the input
   * @throws IOException
   */
  public void readFrom(DataInputStream input) throws IOException {
    int partitionNum = input.readInt();
    LOG.info("partitionNum=" + partitionNum);
    if (LOG.isDebugEnabled()) {
      LOG.debug("readFrom input, matrixId: " + matrixId + ", partitionNum: " + partitionNum);
    }
    for (int i = 0; i < partitionNum; i++) {
      int partitionId = input.readInt();
      LOG.debug("parse partitionId: " + partitionId);
      partitionMaps.get(partitionId).readFrom(input);
    }
  }

  /**
   * Write partitions of matrix to output
   *
   * @param output the output
   * @throws IOException
   */
  public void writeTo(DataOutputStream output) throws IOException {
    output.writeInt(partitionMaps.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "writeTo output, matrixId: " + matrixId + ", martitionSize: " + partitionMaps.size());
    }
    for (Entry<Integer, ServerPartition> entry : partitionMaps.entrySet()) {
      LOG.debug("write partitionId: " + entry.getKey());
      output.writeInt(entry.getKey());
      ServerPartition serverPartition = entry.getValue();
      serverPartition.writeTo(output);
    }
  }

  /**
   * Write matrix id and partition num as header.
   *
   * @param output the output
   * @throws IOException
   */
  public void writeHeader(DataOutputStream output) throws IOException {
    output.writeInt(matrixId);
    output.writeInt(partitionMaps.size());
  }

  /**
   * Gets partition specified by partition id
   *
   * @param partId the part id
   * @return the partition
   */
  public ServerPartition getPartition(int partId) {
    return partitionMaps.get(partId);
  }

  /**
   * Gets partitions of matrix's clocks.
   *
   * @param clocks the clocks
   */
  public void getClocks(Object2IntOpenHashMap<PartitionKey> clocks) {
    for (Entry<Integer, ServerPartition> partEntry : partitionMaps.entrySet()) {
      LOG.debug(
        "partitionKey = " + partEntry.getValue().partitionKey + ", clock = " + partEntry.getValue()
          .getClock());
      clocks.put(partEntry.getValue().partitionKey, partEntry.getValue().getClock());
    }
  }

  /**
   * Sets clock of task.
   *
   * @param taskIndex the task index
   * @param clock     the clock
   */
  public void setClock(int taskIndex, int clock) {
    for (ServerPartition partition : partitionMaps.values()) {
      partition.clock(taskIndex, clock);
      LOG.info("partition " + partition.getPartitionKey() + ", clock is " + partition.getClock());
    }
  }
}
