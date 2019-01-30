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


package com.tencent.angel.ml.matrix;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.ps.storage.partitioner.Partitioner;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * MatrixContext is used for user to set Matrix information.
 */
public class MatrixContext implements Serializable {

  private final static Log LOG = LogFactory.getLog(MatrixContext.class);

  /**
   * Matrix readable name
   */
  private String name;

  /**
   * Number of rows for this matrix
   */
  private int rowNum;

  /**
   * Number of cols for this matrix
   */
  private long colNum;

  /**
   * Index range start
   */
  private long indexStart = 0L;

  /**
   * Index range end
   */
  private long indexEnd;

  /**
   * Number of valid indexes
   */
  private long validIndexNum = 0L;

  /**
   * Number of rows for one block
   */
  private int maxRowNumInBlock;

  /**
   * Number of cols for one block
   */
  private long maxColNumInBlock;

  /**
   * Partitioner for this matrix
   */
  private Class<? extends Partitioner> partitionerClass;

  /**
   * Matrix partitions
   */
  private List<PartContext> parts;

  /**
   * Row type
   */
  private RowType rowType;

  /**
   * Others key value attributes for this matrix.
   */
  private Map<String, String> attributes;

  /**
   * Matrix id
   */
  private int matrixId;

  /**
   * Creates a new MatrixContext by default.
   */
  public MatrixContext() {
    this("", -1, -1, -1, -1, RowType.T_DOUBLE_DENSE);
  }

  /**
   * Create a new MatrixContext
   *
   * @param name   matrix name
   * @param rowNum matrix row number
   * @param colNum matrix column number
   */
  public MatrixContext(String name, int rowNum, long colNum) {
    this(name, rowNum, colNum, -1, -1, -1, RowType.T_DOUBLE_DENSE);
  }

  public MatrixContext(String name, int rowNum, long start, long end) {
    this(name, rowNum, -1, start, end, -1, -1, -1, new ArrayList<>(), RowType.T_DOUBLE_DENSE);
  }


  /**
   * Create a new MatrixContext
   *
   * @param name             matrix name
   * @param rowNum           matrix row number
   * @param colNum           matrix column number
   * @param maxRowNumInBlock matrix block row number
   * @param maxColNumInBlock matrix block column number
   */
  public MatrixContext(String name, int rowNum, long colNum, int maxRowNumInBlock,
                       long maxColNumInBlock) {
    this(name, rowNum, colNum, -1, maxRowNumInBlock, maxColNumInBlock, RowType.T_DOUBLE_DENSE);
  }

  /**
   * Create a new MatrixContext
   *
   * @param name             matrix name
   * @param rowNum           matrix row number
   * @param colNum           matrix column number
   * @param maxRowNumInBlock matrix block row number
   * @param maxColNumInBlock matrix block column number
   * @param rowType          matrix row type
   */
  public MatrixContext(String name, int rowNum, long colNum, int maxRowNumInBlock,
                       long maxColNumInBlock, RowType rowType) {
    this(name, rowNum, colNum, -1, maxRowNumInBlock, maxColNumInBlock, rowType);
  }

  /**
   * Create a new MatrixContext
   *
   * @param name             matrix name
   * @param rowNum           matrix row number
   * @param colNum           matrix column number
   * @param validIndexNum    number of valid indexes
   * @param maxRowNumInBlock matrix block row number
   * @param maxColNumInBlock matrix block column number
   */
  public MatrixContext(String name, int rowNum, long colNum, long validIndexNum,
                       int maxRowNumInBlock, long maxColNumInBlock) {
    this(name, rowNum, colNum, validIndexNum, maxRowNumInBlock, maxColNumInBlock,
            RowType.T_DOUBLE_DENSE);
  }

  /**
   * Create a new MatrixContext
   *
   * @param name             matrix name
   * @param rowNum           matrix row number
   * @param colNum           matrix column number
   * @param validIndexNum    number of valid indexes
   * @param maxRowNumInBlock matrix block row number
   * @param maxColNumInBlock matrix block column number
   * @param rowType          matrix row type
   */
  public MatrixContext(String name, int rowNum, long colNum, long validIndexNum,
                       int maxRowNumInBlock, long maxColNumInBlock, RowType rowType) {
    this(name, rowNum, colNum, -1, -1, validIndexNum, maxRowNumInBlock, maxColNumInBlock,
            new ArrayList<>(), rowType);
  }

  /**
   * Create a new MatrixContext, this method cannot call by users since colnum/ (start, end)
   * cannot be set simultaneously.
   */
  public MatrixContext(String name, int rowNum, long colNum, long indexStart, long indexEnd,
                        long validIndexNum, int maxRowNumInBlock, long maxColNumInBlock,
                        List<PartContext> parts, RowType rowType) {

    // If col == -1 and start/end not set
    if (colNum == -1 && indexEnd <= indexStart) {
      if (rowType.isIntKey()) {
        indexStart = Integer.MIN_VALUE;
        indexEnd = Integer.MAX_VALUE;
        colNum = indexEnd - indexStart;
      } else {
        indexStart = Long.MIN_VALUE;
        indexEnd = Long.MAX_VALUE;
      }
    } else if (colNum == -1 && indexEnd > indexStart) {
      // start/end set
      // for dense type, we need to set the colNum to set dim for vectors
      if (rowType.isIntKey())
        colNum = indexEnd - indexStart;
    } else if (colNum > 0 && indexEnd <= indexStart) {
      // colNum set, start/end not set
      indexStart = 0;
      indexEnd = colNum;
    } else if (colNum > 0 && indexEnd > indexStart) {
      // both set, check its valid
      assert (colNum == indexEnd - indexStart);
    }

    if (colNum == -1 && rowType.isLongKey() && rowType.isDense())
      throw new AngelException("matrix " + name + " is dense and with longkey, might cost a lot of memory. " +
              "Please considering to configure with sparse row type, like (T_FLOAT_SPARSE_LONGKEY)");

    LOG.info("Matrix context " + name + " row=" + rowNum +
            " col=" + colNum + " start=" + indexStart + " end=" + indexEnd);

    this.name = name;
    this.rowNum = rowNum;
    this.colNum = colNum;
    this.indexStart = indexStart;
    this.indexEnd = indexEnd;
    this.validIndexNum = validIndexNum;
    this.maxRowNumInBlock = maxRowNumInBlock;
    this.maxColNumInBlock = maxColNumInBlock;
    this.parts = parts;
    this.rowType = rowType;
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
   * Get number of valid indexes
   *
   * @return number of valid indexes
   */
  public long getValidIndexNum() {
    return validIndexNum;
  }

  /**
   * Set number of valid indexes
   *
   * @param validIndexNum number of valid indexes
   */
  public void setValidIndexNum(long validIndexNum) {
    this.validIndexNum = validIndexNum;
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
  public Class<? extends Partitioner> getPartitionerClass() {
    return partitionerClass;
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public RowType getRowType() {
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
   * Set matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
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
  public void setColNum(long colNum) {
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
  public void setMaxColNumInBlock(long maxColNumInBlock) {
    this.maxColNumInBlock = maxColNumInBlock;
  }

  /**
   * Sets partitioner.
   *
   * @param partitionerClass the partitioner class
   */
  public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
    this.partitionerClass = partitionerClass;
  }

  /**
   * Sets row type.
   *
   * @param rowType the row type
   */
  public void setRowType(RowType rowType) {
    this.rowType = rowType;
  }

  /**
   * Set matrix op log type
   *
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

  private void initPartitioner() {
    if (partitionerClass != null) {
      return;
    }

    partitionerClass = RangePartitioner.class;
  }

  /**
   * Set index range start
   *
   * @param indexStart index range start
   */
  public void setIndexStart(long indexStart) {
    this.indexStart = indexStart;
  }

  /**
   * get index range start
   *
   * @return
   */
  public long getIndexStart() {
    return indexStart;
  }

  /**
   * Get index range end
   *
   * @return index range end
   */
  public long getIndexEnd() {
    return indexEnd;
  }

  /**
   * Set index range end
   *
   * @param indexEnd index range end
   */
  public void setIndexEnd(long indexEnd) {
    this.indexEnd = indexEnd;
  }

  /**
   * Get matrix partitions
   *
   * @return matrix partitions
   */
  public List<PartContext> getParts() {
    return parts;
  }

  /**
   * Set matrix partitions
   *
   * @param parts matrix partitions
   */
  public void setParts(List<PartContext> parts) {
    this.parts = parts;
  }

  /**
   * Add a partition context
   *
   * @param part partition context
   */
  public void addPart(PartContext part) {
    parts.add(part);
  }


  /**
   * Get matrix id
   *
   * @return matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Init matrix
   */
  public void init(Configuration conf) throws IOException {
    initPartitioner();
    check();
    String loadPath = attributes.get(MatrixConf.MATRIX_LOAD_PATH);
    if (loadPath == null) {
      loadPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH);
      if (loadPath != null) {
        if (matrixPathExist(loadPath, name, conf)) {
          attributes.put(MatrixConf.MATRIX_LOAD_PATH, loadPath);
          loadMatrixMetaFromFile(name, loadPath, conf);
        }
      }
    } else {
      loadMatrixMetaFromFile(name, loadPath, conf);
    }
  }

  private boolean matrixPathExist(String loadPath, String name, Configuration conf)
          throws IOException {
    Path matrixPath = new Path(loadPath, name);
    FileSystem fs = matrixPath.getFileSystem(conf);
    return fs.exists(matrixPath);
  }

  private void check() {
    if (rowType == RowType.T_DOUBLE_DENSE || rowType == RowType.T_FLOAT_DENSE
            || rowType == RowType.T_LONG_DENSE || rowType == RowType.T_INT_DENSE) {
      assert colNum > 0;
    }
  }

  private void loadMatrixMetaFromFile(String name, String path, Configuration conf)
          throws IOException {
    Path meteFilePath = new Path(new Path(path, name), ModelFilesConstent.modelMetaFileName);
    MatrixFilesMeta meta = new MatrixFilesMeta();

    FileSystem fs = meteFilePath.getFileSystem(conf);
    LOG.info("Load matrix meta for matrix " + name + " from " + meteFilePath);

    if (!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }

    FSDataInputStream input = fs.open(meteFilePath);
    try {
      meta.read(input);
    } catch (Throwable e) {
      throw new IOException("Read meta failed ", e);
    } finally {
      input.close();
    }

    rowNum = meta.getRow();
    colNum = meta.getCol();
    maxRowNumInBlock = meta.getBlockRow();
    maxColNumInBlock = meta.getBlockCol();
    indexStart = meta.getFeatureIndexStart();
    indexEnd = meta.getFeatureIndexEnd();
    rowType = RowType.valueOf(meta.getRowType());
    Map<String, String> oldAttributes = meta.getOptions();
    if (oldAttributes != null && !oldAttributes.isEmpty()) {
      for (Map.Entry<String, String> kv : oldAttributes.entrySet()) {
        attributes.put(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public String toString() {
    return "MatrixContext{" + "name='" + name + '\'' + ", rowNum=" + rowNum + ", colNum=" + colNum
            + ", validIndexNum=" + validIndexNum + ", maxRowNumInBlock=" + maxRowNumInBlock
            + ", maxColNumInBlock=" + maxColNumInBlock + ", partitionerClass=" + partitionerClass
            + ", rowType=" + rowType + ", attributes=" + attributes + ", matrixId=" + matrixId + '}';
  }

  /**
   * Get estimate sparsity
   *
   * @return estimate sparsity
   */
  public double getEstSparsity() {
    if (validIndexNum <= 0) {
      return 0.0;
    } else {
      if (colNum <= 0) {
        if (rowType == RowType.T_DOUBLE_SPARSE || rowType == RowType.T_FLOAT_SPARSE
                || rowType == RowType.T_LONG_SPARSE || rowType == RowType.T_INT_SPARSE) {
          return (double) validIndexNum / 2 / Integer.MAX_VALUE;
        } else {
          return (double) validIndexNum / 2 / Long.MAX_VALUE;
        }
      } else {
        return (double) validIndexNum / colNum;
      }
    }
  }
}
