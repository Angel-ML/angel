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

package com.tencent.angel.model.output.format;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * The meta data for a Matrix partition.
 */
public class MatrixPartitionMeta {
  private static final Log LOG = LogFactory.getLog(MatrixPartitionMeta.class);
  /**
   * Partition id
   */
  private int partId;

  /**
   * The start row index for this partition
   */
  private int startRow;

  /**
   * The end row index for this partition
   */
  private int endRow;

  /**
   * The start column index for this partition
   */
  private long startCol;

  /**
   * The end column index for this partition
   */
  private long endCol;

  /**
   * Non-zero element number in this partition
   */
  private long nnz;

  /**
   * The file name to which the partition is written
   */
  private String fileName;

  /**
   * The start position for this partition in the file
   */
  private long offset;

  /**
   * Written length
   */
  private long length;

  /**
   * Saved row number
   */
  private int saveRowNum;

  /**
   * rows offset
   */
  private Map<Integer, RowPartitionMeta> rowMetas;

  /**
   * Save column number, just for column first save method
   */
  private int saveColNum;

  /**
   * Save element number for a column, just for column first save method
   */
  private int saveColElemNum;

  /**
   * Create a empty PartitionMeta
   */
  public MatrixPartitionMeta() {
    this(-1, -1, -1, -1, -1, 0, "", 0, 0);
  }

  /**
   * Create a PartitionMeta
   *
   * @param partId   partition index
   * @param startRow partition start row index
   * @param endRow   partition end row index
   * @param startCol partition start column index
   * @param endCol   partition end column index
   * @param nnz      Non-zero element number in this partition
   * @param fileName The file name to which the partition is written
   * @param offset   The start position for this partition in the file
   * @param length   total write bytes
   */
  public MatrixPartitionMeta(int partId, int startRow, int endRow, long startCol, long endCol,
    long nnz, String fileName, long offset, long length) {
    this.partId = partId;
    this.startRow = startRow;
    this.endRow = endRow;
    this.startCol = startCol;
    this.endCol = endCol;
    this.nnz = nnz;
    this.fileName = fileName;
    this.offset = offset;
    this.length = length;
    this.rowMetas = new LinkedHashMap<>();
  }

  public void write(DataOutputStream output) throws IOException {
    output.writeInt(partId);
    output.writeInt(startRow);
    output.writeInt(endRow);
    output.writeLong(startCol);
    output.writeLong(endCol);
    output.writeLong(nnz);
    output.writeInt(saveRowNum);
    output.writeInt(saveColNum);
    output.writeInt(saveColElemNum);
    output.writeUTF(fileName);
    output.writeLong(offset);
    output.writeLong(length);
    if (!rowMetas.isEmpty()) {
      output.writeInt(rowMetas.size());
      for (RowPartitionMeta meta : rowMetas.values()) {
        output.writeInt(meta.getRowId());
        output.writeLong(meta.getOffset());
        output.writeInt(meta.getElementNum());
      }
    } else {
      output.writeInt(0);
    }
  }

  public void read(DataInputStream input) throws IOException {
    partId = input.readInt();
    startRow = input.readInt();
    endRow = input.readInt();
    startCol = input.readLong();
    endCol = input.readLong();
    nnz = input.readLong();
    saveRowNum = input.readInt();
    saveColNum = input.readInt();
    saveColElemNum = input.readInt();
    fileName = input.readUTF();
    offset = input.readLong();
    length = input.readLong();
    int rowIndexNum = input.readInt();
    rowMetas = new LinkedHashMap<>(rowIndexNum);
    for (int i = 0; i < rowIndexNum; i++) {
      RowPartitionMeta rowMeta = new RowPartitionMeta(input.readInt(), input.readLong(), input.readInt());
      rowMetas.put(rowMeta.getRowId(), rowMeta);
    }
  }

  /**
   * Write matrix partition meta to a Json object
   * @return json object
   * @throws IOException
   * @throws JSONException
   */
  public void write(JSONObject jsonObject) throws IOException, JSONException {
    jsonObject.put("partId", partId);
    jsonObject.put("startRow", startRow);
    jsonObject.put("endRow", endRow);
    jsonObject.put("startCol", startCol);
    jsonObject.put("endCol", endCol);
    jsonObject.put("nnz", nnz);
    jsonObject.put("saveRowNum", saveRowNum);
    jsonObject.put("saveColNum", saveColNum);
    jsonObject.put("saveColElemNum", saveColElemNum);
    jsonObject.put("fileName", fileName);
    jsonObject.put("offset", offset);
    jsonObject.put("length", length);
    Map<Integer, JSONObject> rowJsonMap = new HashMap<>();
    for(Map.Entry<Integer, RowPartitionMeta> rowMetaEntry : rowMetas.entrySet()) {
      JSONObject rowJsonObject = new JSONObject();
      rowMetaEntry.getValue().write(rowJsonObject);
      rowJsonMap.put(rowMetaEntry.getKey(), rowJsonObject);
    }
    jsonObject.put("rowMetas", rowJsonMap);
  }

  public void read(JSONObject jsonObject) throws IOException, JSONException {
    partId = jsonObject.getInt("partId");
    startRow = jsonObject.getInt("startRow");
    endRow = jsonObject.getInt("endRow");
    startCol = jsonObject.getLong("startCol");
    endCol = jsonObject.getLong("endCol");
    nnz = jsonObject.getLong("nnz");
    saveRowNum = jsonObject.getInt("saveRowNum");
    saveColNum = jsonObject.getInt("saveColNum");
    saveColElemNum = jsonObject.getInt("saveColElemNum");
    fileName = jsonObject.getString("fileName");
    offset = jsonObject.getLong("offset");
    length = jsonObject.getLong("length");
    JSONObject rowMetasJson = (JSONObject)jsonObject.get("rowMetas");
    Iterator<String> rowKeys = rowMetasJson.keys();
    String key;
    while(rowKeys.hasNext()) {
      key = rowKeys.next();
      JSONObject jb = (JSONObject)rowMetasJson.get(key);
      RowPartitionMeta rowMeta = new RowPartitionMeta(-1, -1, -1);
      rowMeta.read(jb);
      rowMetas.put(rowMeta.getRowId(), rowMeta);
    }
  }


  /**
   * Get partition id
   *
   * @return partition id
   */
  public int getPartId() {
    return partId;
  }

  /**
   * Set partition id
   *
   * @param partId partition id
   */
  public void setPartId(int partId) {
    this.partId = partId;
  }

  /**
   * Get start row index of partition
   *
   * @return start row index of partition
   */
  public int getStartRow() {
    return startRow;
  }

  /**
   * Set start row index of partition
   *
   * @param startRow start row index of partition
   */
  public void setStartRow(int startRow) {
    this.startRow = startRow;
  }

  /**
   * Get end row index of partition
   *
   * @return end row index of partition
   */
  public int getEndRow() {
    return endRow;
  }

  /**
   * Set end row index of partition
   *
   * @param endRow end row index of partition
   */
  public void setEndRow(int endRow) {
    this.endRow = endRow;
  }

  /**
   * Get start column index of partition
   *
   * @return start column index of partition
   */
  public long getStartCol() {
    return startCol;
  }

  /**
   * Set start column index of partition
   *
   * @param startCol start column index of partition
   */
  public void setStartCol(long startCol) {
    this.startCol = startCol;
  }

  /**
   * Get end column index of partition
   *
   * @return end column index of partition
   */
  public long getEndCol() {
    return endCol;
  }

  /**
   * Set end column index of partition
   *
   * @param endCol end column index of partition
   */
  public void setEndCol(long endCol) {
    this.endCol = endCol;
  }

  /**
   * Get non-zero element number of partition
   *
   * @return non-zero element number of partition
   */
  public long getNnz() {
    return nnz;
  }

  /**
   * Set non-zero element number of partition
   *
   * @param nnz non-zero element number of partition
   */
  public void setNnz(int nnz) {
    this.nnz = nnz;
  }

  /**
   * Get the name of file which the partition is written to
   *
   * @return file name
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Set the name of file which the partition is written to
   *
   * @param fileName file name
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * Get the start position of partition in file
   *
   * @return start position
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Set the start position of partition in file
   *
   * @param offset start position
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Gets row metas.
   *
   * @return the row metas
   */
  public Map<Integer, RowPartitionMeta> getRowMetas() {
    return rowMetas;
  }

  /**
   * Sets row meta.
   *
   * @param rowMeta the row meta
   */
  public void setRowMeta(RowPartitionMeta rowMeta) {
    this.rowMetas.put(rowMeta.getRowId(), rowMeta);
  }

  /**
   * Get row meta
   * @param rowId row id
   * @return row meta
   */
  public RowPartitionMeta getRowMeta(int rowId) {
    return rowMetas.get(rowId);
  }

  /**
   * Set non-zero element number
   *
   * @param nnz
   */
  public void setNnz(long nnz) {
    this.nnz = nnz;
  }

  /**
   * Get written len
   *
   * @return Written length
   */
  public long getLength() {
    return length;
  }

  /**
   * Set Written length
   *
   * @param length Written length
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Set the number of saved rows
   * @param saveRowNum the number of saved rows
   */
  public void setSaveRowNum(int saveRowNum) {
    this.saveRowNum = saveRowNum;
  }

  /**
   * Get saved row number
   * @return
   */
  public int getSaveRowNum() {
    return saveRowNum;
  }

  /**
   * Get save column number
   * @return save column number
   */
  public int getSaveColNum() {
    return saveColNum;
  }

  /**
   * Set save column number
   * @param saveColNum save column number
   */
  public void setSaveColNum(int saveColNum) {
    this.saveColNum = saveColNum;
  }

  /**
   * Get element number for a column
   * @return element number for a column
   */
  public int getSaveColElemNum() {
    return saveColElemNum;
  }

  /**
   * Get element number for a column
   * @param saveColElemNum element number for a column
   */
  public void setSaveColElemNum(int saveColElemNum) {
    this.saveColElemNum = saveColElemNum;
  }

  @Override public String toString() {
    return "PartitionMeta{" + "partId=" + partId + ", startRow=" + startRow + ", endRow=" + endRow
      + ", startCol=" + startCol + ", endCol=" + endCol + ", nnz=" + nnz + ", fileName='" + fileName
      + '\'' + ", offset=" + offset + ", length=" + length + '}';
  }
}
