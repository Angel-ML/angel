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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.model.output.format;

import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Model meta file, it includes matrix properties and partition meta
 */
public class ModelFilesMeta {
  private static final Log LOG = LogFactory.getLog(ModelFilesMeta.class);
  /**
   * Matrix id
   */
  private int matrixId;

  /**
   * Matrix row type
   */
  private int rowType;

  /**
   * Row number of matrix
   */
  private int row;

  /**
   * Row number in a block
   */
  private int blockRow;

  /**
   * Matrix column number
   */
  private long col;

  /**
   * Column number in a block
   */
  private long blockCol;

  /**
   * Matrix name
   */
  private String matrixName;

  /**
   * Other matrix parameters
   */
  private Map<String, String> options;

  /**
   * Matrix partition meta
   */
  private Map<Integer, ModelPartitionMeta> partMetas;

  /**
   * Create a MatrixFilesMeta
   * @param matrixId matrix id
   * @param matrixName matrix name
   * @param rowType row type
   * @param row row number
   * @param col cloumn number
   * @param blockRow row number in a block
   * @param blockCol column number in a block
   * @param options other matrix parameters
   * @param partMetas partition meta
   */
  public ModelFilesMeta(int matrixId, String matrixName, int rowType, int row, long col,
    int blockRow, long blockCol, Map<String, String> options,
    Map<Integer, ModelPartitionMeta> partMetas) {
    this.matrixId = matrixId;
    this.matrixName = matrixName;
    this.rowType = rowType;
    this.row = row;
    this.col = col;
    this.blockRow = blockRow;
    this.blockCol = blockCol;
    this.options = options;
    this.partMetas = partMetas;
  }

  /**
   * Create a MatrixFilesMeta
   * @param matrixId matrix id
   * @param matrixName matrix name
   * @param rowType row type
   * @param row row number
   * @param col cloumn number
   * @param blockRow row number in a block
   * @param blockCol column number in a block
   * @param options other matrix parameters
   */
  public ModelFilesMeta(int matrixId, String matrixName, int rowType, int row, long col,
    int blockRow, long blockCol, Map<String, String> options) {
    this(matrixId, matrixName, rowType, row, col, blockRow, blockCol, options, new ConcurrentSkipListMap<>());
  }

  /**
   * Create a empty MatrixFilesMeta
   */
  public ModelFilesMeta() {
    this(-1, "", -1, -1, -1,
      -1, -1, new HashMap<>(), new ConcurrentSkipListMap<>());
  }

  /**
   * Merge server matrix meta to this
   * @param meta server matrix meta
   */
  public void merge(PSModelFilesMeta meta) {
    partMetas.putAll(meta.getPartMetas());
  }

  /**
   * Write matrix meta to output stream
   * @param output output stream
   * @throws IOException
   */
  public void write(DataOutputStream output) throws IOException {
    output.writeInt(matrixId);
    output.writeUTF(matrixName);
    output.writeInt(rowType);
    output.writeInt(row);
    output.writeLong(col);
    output.writeInt(blockRow);
    output.writeLong(blockCol);
    if (options == null || options.isEmpty()) {
      output.writeInt(0);
    } else {
      output.writeInt(options.size());
      for (Map.Entry<String, String> opEntry : options.entrySet()) {
        output.writeUTF(opEntry.getKey());
        output.writeUTF(opEntry.getValue());
      }
    }

    if (partMetas == null || partMetas.isEmpty()) {
      output.writeInt(0);
    } else {
      output.writeInt(partMetas.size());
      for (Map.Entry<Integer, ModelPartitionMeta> partEntry : partMetas.entrySet()) {
        partEntry.getValue().write(output);
      }
    }
  }

  /**
   * Read matrix meta from input stream
   * @param input input stream
   * @throws IOException
   */
  public void read(DataInputStream input) throws IOException {
    matrixId = input.readInt();
    matrixName = input.readUTF();
    rowType = input.readInt();
    row = input.readInt();
    col = input.readLong();
    blockRow = input.readInt();
    blockCol = input.readLong();

    int optionNum = input.readInt();
    options = new HashMap<>();
    for (int i = 0; i < optionNum; i++) {
      options.put(input.readUTF(), input.readUTF());
    }

    int partNum = input.readInt();
    partMetas = new TreeMap<>();
    for (int i = 0; i < partNum; i++) {
      ModelPartitionMeta partMeta = new ModelPartitionMeta();
      partMeta.read(input);
      partMetas.put(partMeta.getPartId(), partMeta);
    }
  }

  /**
   * Get matrix id
   * @return matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Set matrix id
   * @param matrixId matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  /**
   * Get row type
   * @return row type
   */
  public int getRowType() {
    return rowType;
  }

  /**
   * Set row type
   * @param rowType row type
   */
  public void setRowType(int rowType) {
    this.rowType = rowType;
  }

  /**
   * Get row number of matrix
   * @return row number of matrix
   */
  public int getRow() {
    return row;
  }

  /**
   * Set row number of matrix
   * @param row row number of matrix
   */
  public void setRow(int row) {
    this.row = row;
  }

  /**
   * Get row number in matrix block
   * @return row number in matrix block
   */
  public int getBlockRow() {
    return blockRow;
  }

  /**
   * Set row number in matrix block
   * @param blockRow row number in matrix block
   */
  public void setBlockRow(int blockRow) {
    this.blockRow = blockRow;
  }

  /**
   * Get column number of matrix
   * @return column number of matrix
   */
  public long getCol() {
    return col;
  }

  /**
   * Set column number of matrix
   * @param col column number of matrix
   */
  public void setCol(long col) {
    this.col = col;
  }

  /**
   * Get column number in matrix block
   * @return column number in matrix block
   */
  public long getBlockCol() {
    return blockCol;
  }

  /**
   * Set column number in matrix block
   * @param blockCol column number in matrix block
   */
  public void setBlockCol(long blockCol) {
    this.blockCol = blockCol;
  }

  /**
   * Get matrix name
   * @return matrix name
   */
  public String getMatrixName() {
    return matrixName;
  }

  /**
   * Set matrix name
   * @param matrixName matrix name
   */
  public void setMatrixName(String matrixName) {
    this.matrixName = matrixName;
  }

  /**
   * Get matrix other parameters
   * @return matrix other parameters
   */
  public Map<String, String> getOptions() {
    return options;
  }

  /**
   * Set matrix other parameters
   * @param options matrix other parameters
   */
  public void setOptions(Map<String, String> options) {
    this.options = options;
  }

  /**
   * Get matrix partition meta
   * @return matrix partition meta
   */
  public Map<Integer, ModelPartitionMeta> getPartMetas() {
    return partMetas;
  }

  /**
   * Get Model partition meta use part id
   * @param partId partition index
   * @return Model partition meta
   */
  public ModelPartitionMeta getPartMeta(int partId) {
    return partMetas.get(partId);
  }

  @Override public String toString() {
    return "MatrixFilesMeta{" + "matrixId=" + matrixId + ", rowType=" + rowType + ", row=" + row
      + ", blockRow=" + blockRow + ", col=" + col + ", blockCol=" + blockCol + ", matrixName='"
      + matrixName + '\'' + ", options=[" + StringUtils.join(";", options) + "], partMetas=[" + partMetasString() + "]}";
  }

  private String partMetasString() {
    if(partMetas == null || partMetas.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<Integer, ModelPartitionMeta> entry : partMetas.entrySet()) {
      if (first)
        first = false;
      else {
        sb.append(";");
      }
      sb.append("" + entry.getKey() + ":" + entry.getValue());
    }
    return sb.toString();
  }
}
