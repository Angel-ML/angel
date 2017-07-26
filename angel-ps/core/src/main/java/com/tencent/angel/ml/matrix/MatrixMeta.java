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
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixProto;

import java.util.HashMap;
import java.util.Map;

/**
 * The meta of matrix.
 */
public class MatrixMeta {
  private final int id;
  private final String name;
  private final int colNum;
  private final int rowNum;
  private MLProtos.RowType rowType;
  private final Map<String, String> attributes;


  /**
   * Creates a new matrix meta.
   *
   * @param id          the id
   * @param name        the name
   * @param colNum      the col num
   * @param rowNum      the row num
   * @param rowType     the row type
   * @param matrixProto the matrix proto
   */
  public MatrixMeta(int id, String name, int colNum, int rowNum, MLProtos.RowType rowType,
      MatrixProto matrixProto) {
    this.id = id;
    this.name = name;
    this.rowNum = rowNum;
    this.colNum = colNum;
    this.rowType = rowType;
    this.attributes = new HashMap<>();
    for (MLProtos.Pair pair : matrixProto.getAttributeList()) {
      this.attributes.put(pair.getKey(), pair.getValue());
    }
  }

  /**
   * Creates a new matrix meta by proto.
   *
   * @param matrixProto the matrix proto
   */
  public MatrixMeta(MatrixProto matrixProto) {
    this(matrixProto.getId(), matrixProto.getName(), matrixProto.getColNum(), matrixProto
        .getRowNum(), matrixProto.getRowType(), matrixProto);
  }

  /**
   * Creates a new matrix meta from context.
   *
   * @param context  the context
   * @param matrixId the matrix id
   */
  public MatrixMeta(MatrixContext context, int matrixId) {
    this.id = matrixId;
    this.name = context.getName();
    this.rowNum = context.getRowNum();
    this.colNum = context.getColNum();
    this.rowType = context.getRowType();
    this.attributes = context.getAttributes();
  }


  /**
   * Gets id.
   *
   * @return the id
   */
  public int getId() {
    return id;
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
   * Gets name.
   *
   * @return the name
   */
  public String getName() {
    return name;
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
   * Gets attribute.
   *
   * @param key   the key
   * @param value the default value
   * @return the attribute
   */
  public String getAttribute(String key, String value) {
    if (!attributes.containsKey(key))
      return value;
    return attributes.get(key);
  }
  
  /**
   * Gets attribute.
   *
   * @param key   the key
   * @return the attribute
   */
  public String getAttribute(String key) {
    return attributes.get(key);
  }

  /**
   * Is average.
   *
   * @return the result
   */
  public boolean isAverage() {
    String average =
        getAttribute(MatrixConf.MATRIX_AVERAGE, MatrixConf.DEFAULT_MATRIX_AVERAGE);
    return Boolean.parseBoolean(average);
  }

  /**
   * Is hogwild.
   *
   * @return the result
   */
  public boolean isHogwild() {
    String hogwild =
        getAttribute(MatrixConf.MATRIX_HOGWILD, MatrixConf.DEFAULT_MATRIX_HOGWILD);
    return Boolean.parseBoolean(hogwild);
  }

  /**
   * Gets staleness.
   *
   * @return the staleness
   */
  public int getStaleness() {
    int staleness = 0;// MLContext.get().getStaleness();
    if (attributes.containsKey(MatrixConf.MATRIX_STALENESS)) {
      staleness = Integer.parseInt(attributes.get(MatrixConf.MATRIX_STALENESS));
    }
    return staleness;
  }

  @Override
  public String toString() {
    return "MatrixMeta [id=" + id + ", name=" + name + ", colNum=" + colNum + ", rowNum=" + rowNum
        + ", rowType=" + rowType + "]";
  }
}
