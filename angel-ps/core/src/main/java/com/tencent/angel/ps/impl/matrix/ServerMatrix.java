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
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.output.format.*;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.recovery.snapshot.SnapshotRecover;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
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
 */
public class ServerMatrix {

  private final static Log LOG = LogFactory.getLog(ServerMatrix.class);

  /**
   * Mapping from matrix PartitionKey.partitionId to ServerPartition.
   */
  private final HashMap<Integer, ServerPartition> partitionMaps;

  private final int matrixId;

  private final String matrixName;

  private final PSContext context;

  /**
   * The partitions in this PS
   */
  //private final List<PartitionKey> partitionKeys;

  /**
   * Create a new Server matrix by matrix partition.
   *
   * @param matrixMeta the matrix partition contains a set of partitions, which need to load on Parameter Server
   */
  public ServerMatrix(MatrixMeta matrixMeta, PSContext context) {
    this.context = context;

    LOG.info("Creating a Server Matrix, id: " + matrixMeta.getId() + ", name: " + matrixMeta.getName());
    partitionMaps = new HashMap<>( matrixMeta.getPartitionMetas().size());
    matrixId = matrixMeta.getId();
    matrixName = matrixMeta.getName();
  }

  public void init() {
    MatrixMeta matrixMeta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
    Map<Integer, PartitionMeta> partMetas = matrixMeta.getPartitionMetas();
    for (PartitionMeta partMeta : partMetas.values()) {
      ServerPartition part = new ServerPartition(partMeta.getPartitionKey(), matrixMeta.getRowType(), matrixMeta.getEstSparsity());
      partitionMaps.put(partMeta.getPartId(), part);
      part.init();
    }
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
   * Get row split
   * @param partId partition id
   * @param rowId row index
   * @return
   */
  public ServerRow getRow(int partId, int rowId) {
    ServerPartition part = getPartition(partId);
    if(part == null) {
      return null;
    }
    return part.getRow(rowId);
  }

  /**
   * Update related rows of partition which specified by partition key
   *
   * @param partId partition id
   * @param buf          the data buf
   * @throws Exception
   */
  public void update(int partId, ByteBuf buf, RowUpdater updater) throws Exception{
    ServerPartition part = partitionMaps.get(partId);
    if(part == null) {
      return;
    }

    part.update(buf, updater);
  }

  /**
   * Load matrix partitions from files.
   *
   * @throws IOException read files error
   */
  public void load(MatrixMeta matrixMeta, Path path) throws IOException {
    if(path == null) {
      startServering();
      return;
    }

    LOG.info("load matrix " + matrixMeta + " from path " + path);
    Path matrixFilesPath = new Path(path, matrixMeta.getName());
    FileSystem fs = matrixFilesPath.getFileSystem(context.getConf());
    if(!fs.exists(matrixFilesPath)) {
      LOG.warn("Can not find matrix " + matrixMeta.getName() + " in directory " + path);
      startServering();
      return;
    }

    // Read matrix meta from meta file
    PSModelFilesMeta psMatrixFilesMeta = new PSModelFilesMeta(matrixMeta.getId());
    List<ModelPartitionMeta> partFileMetas = new ArrayList<>(partitionMaps.size());
    Path metaFilePath = new Path(matrixFilesPath, ModelFilesConstent.modelMetaFileName);
    if(fs.exists(metaFilePath)) {
      FSDataInputStream input = fs.open(metaFilePath);
      ModelFilesMeta matrixFilesMeta = new ModelFilesMeta();
      matrixFilesMeta.read(input);
      input.close();

      Map<Integer, ModelPartitionMeta> partIdToFileMetaMap = matrixFilesMeta.getPartMetas();
      for (int partId : partitionMaps.keySet()) {
        partFileMetas.add(partIdToFileMetaMap.get(partId));
        psMatrixFilesMeta.addPartitionMeta(partId, partIdToFileMetaMap.get(partId));
      }
    } else {
      Path psMetaFilePath = new Path(matrixFilesPath, ModelFilesConstent.psModelMetaFileName);
      if(fs.exists(psMetaFilePath)) {
        FSDataInputStream input = fs.open(psMetaFilePath);
        psMatrixFilesMeta.read(input);
        input.close();

        Map<Integer, ModelPartitionMeta> partIdToFileMetaMap = psMatrixFilesMeta.getPartMetas();
        for (int partId : partitionMaps.keySet()) {
          partFileMetas.add(partIdToFileMetaMap.get(partId));
          psMatrixFilesMeta.addPartitionMeta(partId, partIdToFileMetaMap.get(partId));
        }
      } else {
        LOG.warn("Can not find matrix meta file in directory " + path);
        startServering();
        return;
      }
    }

    Collections.sort(partFileMetas, new Comparator<ModelPartitionMeta>() {
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

    int size = partFileMetas.size();
    List<Integer> partitionIds = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      partitionIds.add(partFileMetas.get(i).getPartId());
    }

    // Load partitions from file use fork-join
    Vector<String> errorLogs = new Vector<>();
    PartitionDiskOp loadOp =
      new PartitionDiskOp(fs, matrixFilesPath, ACTION.LOAD, partitionIds, psMatrixFilesMeta, errorLogs, 0,
        partitionIds.size(), getMaxFileInSingleFile(context.getConf()));
    context.getIOExecutors().execute(loadOp);
    loadOp.join();
    if (!errorLogs.isEmpty()) {
      String errorLog = "load partitions for matrix " + matrixMeta.getName() + " failed, error log is " + StringUtils.join("\n", errorLogs);
      LOG.error(errorLog);
      throw new IOException(errorLog);
    }
  }

  private int getMaxFileInSingleFile(Configuration conf) {
    return conf
      .getInt(AngelConf.ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE,
        AngelConf.DEFAULT_ANGEL_PS_MAX_PARTITION_NUM_SINGLE_FILE);
  }

  private void startServering() {
    for(ServerPartition part : partitionMaps.values()) {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  public void save(MatrixMeta matrixMeta, List<Integer> partIds, Path outputPath) throws IOException {
    if(partIds == null || partIds.isEmpty()) {
      return;
    }

    FileSystem fs = outputPath.getFileSystem(context.getConf());
    Path matrixFilesPath = new Path(outputPath, matrixMeta.getName());
    if (!fs.mkdirs(matrixFilesPath)) {
      String errorMsg = "can not create output path " + matrixFilesPath + " for matrix " + matrixMeta.getName();
      LOG.error(errorMsg);
      throw new IOException(errorMsg);
    }

    LOG.info("Commit partitions of matrix " + matrixMeta.getName() + " to path " + matrixFilesPath);

    // Save partitions to files use fork-join
    PSModelFilesMeta psMatrixFilesMeta = new PSModelFilesMeta(matrixMeta.getId());

    Collections.sort(partIds, new Comparator<Integer>() {
      @Override public int compare(Integer id1, Integer id2) {
        return id1 - id2;
      }
    });

    Vector<String> errorLogs = new Vector<>();
    PartitionDiskOp commitOp = new PartitionDiskOp(fs, matrixFilesPath, ACTION.SAVE, partIds,
      psMatrixFilesMeta, errorLogs, 0, partIds.size(), getMaxFileInSingleFile(context.getConf()));
    context.getIOExecutors().execute(commitOp);
    commitOp.join();
    if(!errorLogs.isEmpty()) {
      throw new IOException(StringUtils.join("\n", errorLogs));
    }

    // Write the ps matrix meta to the meta file
    Path metaFile = new Path(matrixFilesPath, ModelFilesConstent.psModelMetaFileName);
    Path tmpMetaFile = HdfsUtil.toTmpPath(metaFile);
    FSDataOutputStream metaOut = fs.create(tmpMetaFile);
    psMatrixFilesMeta.write(metaOut);
    metaOut.flush();
    metaOut.close();
    LOG.info("Rename from " + tmpMetaFile.toString() + " to " + metaFile);
    HdfsUtil.rename(tmpMetaFile, metaFile, fs);
  }

  enum ACTION {
    LOAD, SAVE
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
    private final int maxFileInSingleFile;

    public PartitionDiskOp(FileSystem fs, Path matrixPath, ACTION action,
      List<Integer> partitionIds, PSModelFilesMeta serverMatrixMeta,
      Vector<String> errorMsgs, int startPos, int endPos, int maxFileInSingleFile) {
      this.fs = fs;
      this.matrixPath = matrixPath;
      this.action = action;
      this.partitionIds = partitionIds;
      this.serverMatrixMeta = serverMatrixMeta;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
      this.maxFileInSingleFile = maxFileInSingleFile;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos <= maxFileInSingleFile) {
        try {
          process(matrixPath, fs, action, partitionIds, startPos, endPos, serverMatrixMeta);
        } catch (Throwable x) {
          LOG.error(action + " model partitions failed.", x);
          errorMsgs.add(action + " model partitions failed." + x.getMessage());
        }
      } else {
        int middle = (startPos + endPos) / 2;
        PartitionDiskOp opLeft =
          new PartitionDiskOp(fs, matrixPath, action, partitionIds, serverMatrixMeta, errorMsgs,
            startPos, middle, maxFileInSingleFile);
        PartitionDiskOp opRight =
          new PartitionDiskOp(fs, matrixPath, action, partitionIds, serverMatrixMeta, errorMsgs,
            middle, endPos, maxFileInSingleFile);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(Path matrixPath, FileSystem fs, ACTION action,
    List<Integer> partitionIds, int startPos, int endPos,
    PSModelFilesMeta serverMatrixMeta) throws IOException {
    switch (action) {
      case SAVE:
        savePartitions(matrixPath, fs, partitionIds, startPos, endPos, serverMatrixMeta);
        break;

      case LOAD:
        loadPartitions(matrixPath, fs, partitionIds, startPos, endPos, serverMatrixMeta);
        break;

      default:
        break;
    }
  }

  private void savePartitions(Path matrixPath, FileSystem fs,
    List<Integer> partitionIds, int startPos, int endPos,
    PSModelFilesMeta serverMatrixMeta) throws IOException {

    Path destFile = new Path(matrixPath, ModelFilesUtils.fileName(context.getPs().getServerId(), partitionIds.get(startPos)));
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
      partition.save(out, partMeta);
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
      partitionMaps.get(partitionId).load(input);
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
      serverPartition.save(output);
    }
  }
}
