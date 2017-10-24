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

package com.tencent.angel.ml.matrix;

import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.ModelFilesMeta;
import com.tencent.angel.model.output.format.ModelPartitionMeta;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.LongKeyPartitioner;
import com.tencent.angel.ps.PSPartitioner;
import com.tencent.angel.ps.Partitioner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MatrixContext is used for user to set Matrix information.
 */
public class MatrixContext {
  private final static Log LOG = LogFactory.getLog(MatrixContext.class);

  private final static AtomicInteger idGenerator = new AtomicInteger(0);

  /** Matrix readable name */
  private String name;

  /** Number of rows for this matrix */
  private int rowNum;

  /** Number of cols for this matrix */
  private long colNum;

  /** Number of rows for one block */
  private int maxRowNumInBlock;

  /** Number of cols for one block */
  private long maxColNumInBlock;

  /** Partitioner for this matrix  */
  private Partitioner partitioner;

  /** Row type */
  private MLProtos.RowType rowType;

  /** HDFS path for this matrix, if this is set, ps will load matrix from this path before training. */
  private String path;

  /** Others key value attributes for this matrix. */
  private Map<String, String> attributes;

  /** Matrix id */
  private int matrixId;

  /**
   * Creates a new Matrix context by default.
   */
  public MatrixContext() {
    this("", -1, -1, -1, -1);
  }

  public MatrixContext(String name, int rowNum, long colNum) {
    this(name, rowNum, colNum, -1, -1);
  }

  public MatrixContext(String name, int rowNum, long colNum, int maxRowNumInBlock, long maxColNumInBlock) {
    this.name = name;
    this.rowNum = rowNum;
    this.colNum = colNum;
    this.maxRowNumInBlock = maxRowNumInBlock;
    this.maxColNumInBlock = maxColNumInBlock;
    this.rowType = MLProtos.RowType.T_DOUBLE_DENSE;
    this.attributes = new HashMap<>();
    this.matrixId = -1;
  }

  /**
   * Gets name.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Gets row num.
   *
   * @return the row num
   */
  public int getRowNum() {
    return rowNum;
  }

  /**
   * Gets col num.
   *
   * @return the col num
   */
  public long getColNum() {
    return colNum;
  }

  /**
   * Gets max row num in block.
   *
   * @return the max row num in block
   */
  public int getMaxRowNumInBlock() {
    return maxRowNumInBlock;
  }

  /**
   * Gets max col num in block.
   *
   * @return the max col num in block
   */
  public long getMaxColNumInBlock() {
    return maxColNumInBlock;
  }

  /**
   * Gets partitioner.
   *
   * @return the partitioner
   */
  public Partitioner getPartitioner() {
    return partitioner;
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public MLProtos.RowType getRowType() {
    return rowType;
  }

  /**
   * Gets attributes.
   *
   * @return the attributes
   */
  public Map<String, String> getAttributes() {
    return attributes;
  }

  /**
   * Sets name.
   *
   * @param name the name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets row num.
   *
   * @param rowNum the row num
   */
  public void setRowNum(int rowNum) {
    this.rowNum = rowNum;
  }

  /**
   * Sets col num.
   *
   * @param colNum the col num
   */
  public void setColNum(int colNum) {
    this.colNum = colNum;
  }

  /**
   * Sets max row num in block.
   *
   * @param maxRowNumInBlock the max row num in block
   */
  public void setMaxRowNumInBlock(int maxRowNumInBlock) {
    this.maxRowNumInBlock = maxRowNumInBlock;
  }

  /**
   * Sets max col num in block.
   *
   * @param maxColNumInBlock the max col num in block
   */
  public void setMaxColNumInBlock(int maxColNumInBlock) {
    this.maxColNumInBlock = maxColNumInBlock;
  }

  /**
   * Sets partitioner.
   *
   * @param partitioner the partitioner
   */
  public void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

  /**
   * Sets row type.
   *
   * @param rowType the row type
   */
  public void setRowType(MLProtos.RowType rowType) {
    this.rowType = rowType;
  }

  /**
   * Set matrix op log type
   * @param type op log type
   */
  public void setMatrixOpLogType(MatrixOpLogType type) {
    attributes.put(MatrixConf.MATRIX_OPLOG_TYPE, type.name());
  }

  /**
   * Set matrix context.
   *
   * @param key   the key
   * @param value the value
   * @return the matrix context
   */
  public MatrixContext set(String key, String value) {
    attributes.put(key, value);
    return this;
  }

  /**
   * Build mat proto ml protos . matrix proto.
   *
   * @param conf the conf
   * @return the ml protos . matrix proto
   * @throws IOException the io exception
   */
  public MLProtos.MatrixProto buildMatProto(Configuration conf) throws IOException {
    matrixId = idGenerator.incrementAndGet();
    String loadPath = attributes.get(MatrixConf.MATRIX_LOAD_PATH);
    if(partitioner == null) {
      initPartitioner();
    }
    partitioner.init(this, conf);
    List<MLProtos.Partition> partitions;
    if (loadPath != null) {
      partitions = loadPartitionInfoFromHDFS(loadPath, conf);
    } else {
      partitions = partitioner.getPartitions();
    }

    String errorInfo = checkMatrixParams();
    if (!errorInfo.isEmpty()) {
      LOG.error("build matrix failed:" + errorInfo);
      throw new IOException("matrix parameters are not valid:" + errorInfo);
    }

    if(partitions == null || partitions.isEmpty()) {
      throw new IOException("matrix partitions are not valid.");
    }

    return ProtobufUtil.generateMatrixProto(this, partitions);
  }

  private void initPartitioner() {
    if(rowType == RowType.T_DOUBLE_SPARSE_LONGKEY) {
      partitioner = new LongKeyPartitioner();
    } else {
      partitioner = new PSPartitioner();
    }
  }

  /**
   * Gets part id from path.
   *
   * @param path the path
   * @return the part id from path
   */
  private int getPartIdFromPath(String path) {
    String[] parts = path.split("/");
    return Integer.parseInt(parts[parts.length - 1]);
  }

  private String checkMatrixParams() {
    StringBuilder sb = new StringBuilder();
    if(name == null || name.isEmpty()) {
      sb.append("matrix name must not be empty");
      sb.append("\n");
    }
    if(rowNum <= 0 || rowNum > Integer.MAX_VALUE) {
      sb.append("matrix row number must > 0 and <= " + Integer.MAX_VALUE + ", but is ").append(rowNum);
      sb.append("\n");
    }
    if(rowNum > 0 && maxRowNumInBlock > rowNum) {
      sb.append("matrix block row number must > 0 and < ").append(rowNum).append(", but is ").append(maxRowNumInBlock);
      sb.append("\n");
    }

    if(rowType != RowType.T_DOUBLE_SPARSE_LONGKEY)  {
      if(colNum <= 0 || colNum > Integer.MAX_VALUE) {
        sb.append("matrix column number must > 0 and <= " + Integer.MAX_VALUE + ", but is ").append(colNum);
        sb.append("\n");
      }
    }

    if(colNum > 0 && maxColNumInBlock > colNum) {
      sb.append("matrix block column number must > 0 and < ").append(colNum).append(", but is ").append(maxColNumInBlock);
    }
    return sb.toString();
  }


  /**
   * Load matrix proto from hdfs.
   *
   * @param path the path
   * @param conf the conf
   * @return matrix partitions
   * @throws IOException the io exception
   */
  private List<MLProtos.Partition> loadPartitionInfoFromHDFS(String path, Configuration conf) throws IOException {
    Path meteFilePath = new Path(new Path(path, name), ModelFilesConstent.modelMetaFileName);
    ModelFilesMeta meta = new ModelFilesMeta();

    FileSystem fs  = meteFilePath.getFileSystem(conf);
    LOG.info("Load matrix meta for matrix " + name);

    if(!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }

    FSDataInputStream input = fs.open(meteFilePath);
    meta.read(input);

    LOG.info("matrix " + name + " meta=" + meta);
    rowNum = meta.getRow();
    colNum = meta.getCol();
    maxRowNumInBlock = meta.getBlockRow();
    maxColNumInBlock = meta.getBlockCol();
    rowType = RowType.valueOf(meta.getRowType());
    Map<String, String> oldAttributes = meta.getOptions();
    if(oldAttributes != null && !oldAttributes.isEmpty()) {
      for(Map.Entry<String, String> kv : oldAttributes.entrySet()) {
        attributes.putIfAbsent(kv.getKey(), kv.getValue());
      }
    }

    List<MLProtos.Partition> matrixPartitions = new ArrayList<>();
    Map<Integer, ModelPartitionMeta> partMetas = meta.getPartMetas();
    for(Map.Entry<Integer, ModelPartitionMeta> partMetaEntry:partMetas.entrySet()) {
      MLProtos.Partition.Builder partBuilder = MLProtos.Partition.newBuilder();
      partBuilder.setMatrixId(matrixId);
      partBuilder.setPartitionId(partMetaEntry.getKey());
      partBuilder.setStartRow(partMetaEntry.getValue().getStartRow());
      partBuilder.setStartCol(partMetaEntry.getValue().getStartCol());
      partBuilder.setEndRow(partMetaEntry.getValue().getEndRow());
      partBuilder.setEndCol(partMetaEntry.getValue().getEndCol());
      matrixPartitions.add(partBuilder.build());
    }
    return matrixPartitions;
  }

  public int getId() {
    return matrixId;
  }
}
