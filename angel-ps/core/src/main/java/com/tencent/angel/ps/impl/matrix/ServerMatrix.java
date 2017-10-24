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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.model.output.format.*;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.MLProtos.Partition;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.MatrixPartition;
import com.tencent.angel.ps.impl.MatrixDiskIOExecutors;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.StringUtils;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

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
  private final HashMap<Integer, ServerPartition> partitionMaps;

  /**
   * Matrix parameters
   */
  private final Map<String, String> attribute;

  /**
   * Matrix id
   */
  private final int matrixId;

  /**
   * Matrix name
   */
  private final String matrixName;

  /**
   * The partitions in this PS
   */
  private final List<PartitionKey> partitionKeys;

  /**
   * The maximum number of partitions that write to a single file
   */
  private static final int maxPartNumInAFile;

  static {
    if (PSContext.get().getPs() != null && PSContext.get().getConf() != null) {
      maxPartNumInAFile = PSContext.get().getConf()
        .getInt(AngelConf.ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE,
          AngelConf.DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE);
    } else {
      maxPartNumInAFile = AngelConf.DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE;
    }
  }

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
  public void load() throws IOException {
    String path = attribute.get(MatrixConf.MATRIX_LOAD_PATH);
    if (path == null || partitionMaps.isEmpty()) {
      return;
    }

    Configuration conf = PSContext.get().getConf();
    Path matrixPath = new Path(path, matrixName);

    // Read matrix meta from meta file
    Path metaFilePath = new Path(matrixPath, ModelFilesConstent.modelMetaFileName);
    FileSystem fs = metaFilePath.getFileSystem(conf);

    FSDataInputStream input = fs.open(metaFilePath);
    ModelFilesMeta matrixMeta = new ModelFilesMeta();
    matrixMeta.read(input);
    input.close();

    Map<Integer, ModelPartitionMeta> partMetas = matrixMeta.getPartMetas();
    List<ModelPartitionMeta> partMetaList = new ArrayList<>(partitionMaps.size());

    PSModelFilesMeta serverMatrixMeta = new PSModelFilesMeta(matrixMeta.getMatrixId());
    for (int partId : partitionMaps.keySet()) {
      partMetaList.add(partMetas.get(partId));
      serverMatrixMeta.addPartitionMeta(partId, partMetas.get(partId));
    }

    Collections.sort(partMetaList, new Comparator<ModelPartitionMeta>() {
      @Override public int compare(ModelPartitionMeta p1, ModelPartitionMeta p2) {
        if(p1.getFileName().compareTo(p2.getFileName()) < 0) {
          return -1;
        } else if(p1.getFileName().compareTo(p2.getFileName()) > 0) {
          return 1;
        } else {
          return (int)(p1.getOffset() - p2.getOffset());
        }
      }
    });

    int size = partMetaList.size();
    List<Integer> partitionIds = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      LOG.info("======partMetaList[" + i + "]=" + partMetaList.get(i));
      partitionIds.add(partMetaList.get(i).getPartId());
    }

    // Load partitions from file use fork-join
    Vector<String> errorLogs = new Vector<>();
    PartitionDiskOp loadOp =
      new PartitionDiskOp(fs, matrixPath, ACTION.LOAD, partitionIds, serverMatrixMeta, errorLogs, 0,
        partitionIds.size());
    MatrixDiskIOExecutors.execute(loadOp);
    loadOp.join();
    if (!errorLogs.isEmpty()) {
      String errorLog = "load partitions for matrix " + matrixName + " failed, error log is " + StringUtils.join("\n", errorLogs);
      LOG.error(errorLog);
      throw new IOException(errorLog);
    }
  }

  public void commit(FileSystem fs, Path outputPath) throws IOException {
    Path matrixPath = new Path(outputPath, matrixName);
    if (!fs.mkdirs(matrixPath)) {
      String errorMsg = "can not create output path " + matrixPath + " for matrix " + matrixName;
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    LOG.info("Commit partitions of matrix " + matrixName + " to path " + matrixPath);

    // Save partitions to files use fork-join
    PSModelFilesMeta matrixMeta = new PSModelFilesMeta(matrixId);
    List<Integer> partitionIds = new ArrayList<>(partitionMaps.keySet());

    Collections.sort(partitionIds, new Comparator<Integer>() {
      @Override public int compare(Integer id1, Integer id2) {
        return id1 - id2;
      }
    });

    Vector<String> errorLogs = new Vector<>();
    PartitionDiskOp commitOp = new PartitionDiskOp(fs, matrixPath, ACTION.COMMIT, partitionIds, matrixMeta, errorLogs, 0, partitionIds.size());
    MatrixDiskIOExecutors.execute(commitOp);
    commitOp.join();
    if(!errorLogs.isEmpty()) {
      throw new IOException(StringUtils.join("\n", errorLogs));
    }

    // Write the ps matrix meta to the meta file
    Path metaFile = new Path(matrixPath, ModelFilesConstent.psModelMetaFileName);
    Path tmpMetaFile = HdfsUtil.toTmpPath(metaFile);
    FSDataOutputStream metaOut = fs.create(tmpMetaFile, (short) 1);
    matrixMeta.write(metaOut);
    metaOut.flush();
    metaOut.close();
    HdfsUtil.rename(tmpMetaFile, metaFile, fs);
  }

  enum ACTION {
    LOAD, COMMIT
  }


  class PartitionDiskOp extends RecursiveAction {
    private final Path matrixPath;
    private final FileSystem fs;
    private final List<Integer> partitionIds;
    private final PSModelFilesMeta serverMatrixMeta;
    private final Vector<String> errorMsgs;
    private final int startPos;
    private final int endPos;
    private final ACTION action;

    public PartitionDiskOp(FileSystem fs, Path matrixPath, ACTION action,
      List<Integer> partitionIds, PSModelFilesMeta serverMatrixMeta,
      Vector<String> errorMsgs, int startPos, int endPos) {
      this.fs = fs;
      this.matrixPath = matrixPath;
      this.action = action;
      this.partitionIds = partitionIds;
      this.serverMatrixMeta = serverMatrixMeta;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos <= maxPartNumInAFile) {
        try {
          process(matrixPath, fs, action, partitionIds, startPos, endPos, serverMatrixMeta);
        } catch (Throwable x) {
          LOG.error("load model partitions failed.", x);
          errorMsgs.add("load model partitions failed." + x.getMessage());
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PartitionDiskOp opLeft =
          new PartitionDiskOp(fs, matrixPath, action, partitionIds, serverMatrixMeta, errorMsgs,
            startPos, middle);
        PartitionDiskOp opRight =
          new PartitionDiskOp(fs, matrixPath, action, partitionIds, serverMatrixMeta, errorMsgs,
            middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(Path matrixPath, FileSystem fs, ACTION action,
    List<Integer> partitionIds, int startPos, int endPos,
    PSModelFilesMeta serverMatrixMeta) throws IOException {
    switch (action) {
      case COMMIT:
        commitPartitions(matrixPath, fs, partitionIds, startPos, endPos, serverMatrixMeta);
        break;

      case LOAD:
        loadPartitions(matrixPath, fs, partitionIds, startPos, endPos, serverMatrixMeta);
        break;

      default:
        break;
    }
  }

  private void commitPartitions(Path matrixPath, FileSystem fs,
    List<Integer> partitionIds, int startPos, int endPos,
    PSModelFilesMeta serverMatrixMeta) throws IOException {

    Path destFile = new Path(matrixPath, ModelFilesUtils.fileName(PSContext.get().getPs().getServerId(), partitionIds.get(startPos)));
    Path tmpDestFile = HdfsUtil.toTmpPath(destFile);

    FSDataOutputStream out = fs.create(tmpDestFile);
    long streamPos = 0;
    ServerPartition partition = null;
    for (int i = startPos; i < endPos; i++) {
      LOG.info("Write partition " + partitionIds.get(i) + " of matrix " + matrixName + " to "
        + tmpDestFile);
      streamPos = out.getPos();
      partition = partitionMaps.get(partitionIds.get(i));
      PartitionKey partKey = partition.getPartitionKey();
      ModelPartitionMeta partMeta = new ModelPartitionMeta(partKey.getPartitionId(), partKey.getStartRow(),
        partKey.getEndRow(), partKey.getStartCol(), partKey.getEndCol(), partition.elementNum(),
        destFile.getName(), streamPos, 0);
      partition.commit(out);
      partMeta.setLength(out.getPos() - streamPos);
      serverMatrixMeta.addPartitionMeta(partitionIds.get(i), partMeta);
    }
    out.flush();
    out.close();
    HdfsUtil.rename(tmpDestFile, destFile, fs);
  }

  private void loadPartitions(Path matrixPath, FileSystem fs,
    List<Integer> partitionIds, int startPos, int endPos,
    PSModelFilesMeta serverMatrixMeta) throws IOException {

    ServerPartition partition = null;
    FSDataInputStream input = null;
    long offset = 0;
    String currentFileName = "";
    for(int i = startPos; i < endPos; i++) {
      partition = partitionMaps.get(partitionIds.get(i));
      ModelPartitionMeta partMeta = serverMatrixMeta.getPartitionMeta(partitionIds.get(i));
      String fileName = partMeta.getFileName();
      offset = partMeta.getOffset();
      if(!fileName.equals(currentFileName)) {
        currentFileName = fileName;
        if(input != null) {
          input.close();
        }
        input = fs.open(new Path(matrixPath, currentFileName));

      }
      input.seek(offset);
      partition.load(input);
    }

    if(input != null) {
      input.close();
    }
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
  public void readSnapshot(DataInputStream input) throws IOException {
    int partitionNum = input.readInt();
    LOG.info("partitionNum=" + partitionNum);
    if (LOG.isDebugEnabled()) {
      LOG.debug("readFrom input, matrixId: " + matrixId + ", partitionNum: " + partitionNum);
    }
    for (int i = 0; i < partitionNum; i++) {
      int partitionId = input.readInt();
      LOG.debug("parse partitionId: " + partitionId);
      partitionMaps.get(partitionId).readSnapshot(input);
    }
  }

  /**
   * Write partitions of matrix to output
   *
   * @param output the output
   * @throws IOException
   */
  public void writeSnapshot(DataOutputStream output) throws IOException {
    output.writeInt(partitionMaps.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "writeTo output, matrixId: " + matrixId + ", martitionSize: " + partitionMaps.size());
    }
    for (Map.Entry<Integer, ServerPartition> entry : partitionMaps.entrySet()) {
      LOG.debug("write partitionId: " + entry.getKey());
      output.writeInt(entry.getKey());
      ServerPartition serverPartition = entry.getValue();
      serverPartition.writeSnapshot(output);
    }
  }

  /**
   * Write matrix id and partition num as header.
   *
   * @param output the output
   * @throws IOException
   */
  private void writeHeader(DataOutputStream output) throws IOException {
    output.writeInt(matrixId);
    output.writeUTF(matrixName);
    output.writeInt(partitionMaps.size());
  }

  /**
   * Filter file header
   *
   * @param input input stream
   * @throws IOException
   */
  private void filterHeader(DataInputStream input) throws IOException {
    input.readInt();
    input.readUTF();
    input.readInt();
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
    for (Map.Entry<Integer, ServerPartition> partEntry : partitionMaps.entrySet()) {
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

  /**
   * Get matrix name
   *
   * @return matrix name
   */
  public String getMatrixName() {
    return matrixName;
  }
}
