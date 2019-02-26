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

public class PartContext {
  private int startRow;
  private int endRow;
  private long startCol;
  private long endCol;
  private int indexNum;

  public PartContext(int startRow, int endRow, long startCol, long endCol, int indexNum) {
    this.startRow = startRow;
    this.endRow = endRow;
    this.startCol = startCol;
    this.endCol = endCol;
    this.indexNum = indexNum;
  }

  public int getStartRow() {
    return startRow;
  }

  public void setStartRow(int startRow) {
    this.startRow = startRow;
  }

  public int getEndRow() {
    return endRow;
  }

  public void setEndRow(int endRow) {
    this.endRow = endRow;
  }

  public long getStartCol() {
    return startCol;
  }

  public void setStartCol(long startCol) {
    this.startCol = startCol;
  }

  public long getEndCol() {
    return endCol;
  }

  public void setEndCol(long endCol) {
    this.endCol = endCol;
  }

  public int getIndexNum() {
    return indexNum;
  }

  public void setIndexNum(int indexNum) {
    this.indexNum = indexNum;
  }

  @Override
  public String toString() {
    return "PartContext{" +
        "startRow=" + startRow +
        ", endRow=" + endRow +
        ", startCol=" + startCol +
        ", endCol=" + endCol +
        ", indexNum=" + indexNum +
        '}';
  }
}
