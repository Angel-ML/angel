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

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.PSPartitioner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

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
  private int colNum;

  /** Number of rows for one block */
  private int maxRowNumInBlock;

  /** Number of cols for one block */
  private int maxColNumInBlock;

  /** Partitioner for this matrix  */
  private PSPartitioner partitioner;

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

  public MatrixContext(String name, int rowNum, int colNum) {
    this(name, rowNum, colNum, -1, -1);
  }

  public MatrixContext(String name, int rowNum, int colNum, int maxRowNumInBlock,
      int maxColNumInBlock) {
    this.name = name;
    this.rowNum = rowNum;
    this.colNum = colNum;
    this.maxRowNumInBlock = maxRowNumInBlock;
    this.maxColNumInBlock = maxColNumInBlock;
    this.rowType = MLProtos.RowType.T_DOUBLE_DENSE;
    this.attributes = new HashMap<>();
    this.partitioner = new PSPartitioner();
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
  public int getColNum() {
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
  public int getMaxColNumInBlock() {
    return maxColNumInBlock;
  }

  /**
   * Gets partitioner.
   *
   * @return the partitioner
   */
  public PSPartitioner getPartitioner() {
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
  public void setPartitioner(PSPartitioner partitioner) {
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
    String loadPath = attributes.get(MatrixConfiguration.MATRIX_LOAD_PATH);
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
    if(rowNum <= 0) {
      sb.append("matrix row number must > 0, but is " + rowNum);
      sb.append("\n");
    }
    if(colNum <= 0) {
      sb.append("matrix column number must > 0, but is " + colNum);
      sb.append("\n");
    }
    if(rowNum > 0 && maxRowNumInBlock > rowNum) {
      sb.append("matrix block row number must > 0 and < " + rowNum + ", but is " + maxRowNumInBlock);
      sb.append("\n");
    }
    if(colNum > 0 && maxColNumInBlock > colNum) {
      sb.append("matrix block column number must > 0 and < " + colNum + ", but is " + maxColNumInBlock);
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

    Path inputPath = new Path(path, name);
    FileSystem fs  = inputPath.getFileSystem(conf);

    LOG.info("Load matrix " + name + " from path " + inputPath);

    if(!fs.exists(inputPath)) {
      throw new IOException("matrix path " + inputPath + " does not exist ");
    }
    
    FileStatus[] statuses;
    statuses = fs.listStatus(inputPath);

    if (statuses.length == 0) {
      throw new IOException("there are no partition files in " + inputPath);
    }

    List<MLProtos.Partition> matrixPartitions = new ArrayList<>();
    int hdfsRowNum = Integer.MIN_VALUE;
    int hdfsColNum = Integer.MIN_VALUE;

    int psNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);

    for (int i = 0; i < statuses.length; i ++) {
      FSDataInputStream in = fs.open(statuses[i].getPath());
      in.readInt();
      in.readInt();

      int pid = getPartIdFromPath(statuses[i].getPath().getName());

      int startRow = in.readInt();
      int startCol = in.readInt();
      int endRow   = in.readInt();
      int endCol   = in.readInt();
      
      if(i == 0) {
        RowType type = RowType.valueOf(in.readUTF());
        if(rowType != type) {
          LOG.warn("matrix " + name + " rowtype load from file is " + type);
          rowType = type;
        }
      }

      if (hdfsRowNum < endRow)
        hdfsRowNum = endRow;
      if (hdfsColNum < endCol)
        hdfsColNum = endCol;

      MLProtos.Partition.Builder partBuilder = MLProtos.Partition.newBuilder();
      partBuilder.setMatrixId(matrixId);
      partBuilder.setPartitionId(pid);
      partBuilder.setStartRow(startRow);
      partBuilder.setStartCol(startCol);
      partBuilder.setEndRow(endRow);
      partBuilder.setEndCol(endCol);
      matrixPartitions.add(partBuilder.build());

      LOG.info(String.format("read partition pid=%d startRow=%d startCol=%d endRow=%d endCol=%d",
              pid, startRow, startCol, endRow, endCol));
    }

    if (hdfsRowNum != rowNum) {
      LOG.warn(String.format("parsed row num %d while set row num %d", hdfsRowNum, rowNum));
      rowNum = hdfsRowNum;
    }

    if (hdfsColNum != colNum) {
      LOG.warn(String.format("parsed col num %d while set col num %d", hdfsColNum, colNum));
      colNum = hdfsColNum;
    }

    return matrixPartitions;
  }

  public int getId() {
    return matrixId;
  }
}
