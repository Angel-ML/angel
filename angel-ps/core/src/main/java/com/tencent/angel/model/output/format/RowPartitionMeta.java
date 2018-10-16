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

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The meta data for row split
 */
public class RowPartitionMeta {
  /**
   * Row id
   */
  private int rowId;

  /**
   * The offset for data file for this row split
   */
  private long offset;

  /**
   * The element number for this row split
   */
  private int elementNum;

  /**
   * Save type: 0 dense, 1 sparse. just for SnapshotFormat now
   */
  private int saveType;

  /**
   * Create meta for row split
   *
   * @param rowId      row split
   * @param offset     data offset in the saved file
   * @param elementNum save element number
   * @param saveType   save type : 0 dense, 1 sparse
   */
  public RowPartitionMeta(int rowId, long offset, int elementNum, int saveType) {
    this.rowId = rowId;
    this.offset = offset;
    this.elementNum = elementNum;
    this.saveType = saveType;
  }

  /**
   * Create meta for row split
   *
   * @param rowId      row split
   * @param offset     data offset in the saved file
   * @param elementNum save element number
   */
  public RowPartitionMeta(int rowId, long offset, int elementNum) {
    this(rowId, offset, elementNum, 0);
  }

  /**
   * Write row split meta to output stream use binary format
   *
   * @param output output stream
   * @throws IOException
   */
  public void write(DataOutputStream output) throws IOException {
    output.writeInt(rowId);
    output.writeLong(offset);
    output.writeInt(elementNum);
    output.writeInt(saveType);
  }

  /**
   * Read row split meta from input stream use binary format
   *
   * @param input input stream
   * @throws IOException
   */
  public void read(DataInputStream input) throws IOException {
    rowId = input.readInt();
    offset = input.readLong();
    elementNum = input.readInt();
    saveType = input.readInt();
  }

  /**
   * Write row split meta to a Json object
   *
   * @return json object
   * @throws IOException
   * @throws JSONException
   */
  public void write(JSONObject rowJsonObject) throws IOException, JSONException {
    rowJsonObject.put("rowId", rowId);
    rowJsonObject.put("offset", offset);
    rowJsonObject.put("elementNum", elementNum);
    rowJsonObject.put("saveType", saveType);
  }

  /**
   * Read row split meta from a Json object
   *
   * @param jsonObject json object
   * @throws IOException
   * @throws JSONException
   */
  public void read(JSONObject jsonObject) throws IOException, JSONException {
    rowId = jsonObject.getInt("rowId");
    offset = jsonObject.getInt("offset");
    elementNum = jsonObject.getInt("elementNum");
    saveType = jsonObject.getInt("saveType");
  }

  /**
   * Get the row id for the row split
   *
   * @return row id
   */
  public int getRowId() {
    return rowId;
  }

  /**
   * Set the row id for the row split
   *
   * @param rowId row id
   */
  public void setRowId(int rowId) {
    this.rowId = rowId;
  }

  /**
   * Get the data offset in the saved file for this row split
   *
   * @return the data offset in the saved file for this row split
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Set the data offset in the saved file for this row split
   *
   * @param offset the data offset in the saved file for this row split
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Get save element number
   *
   * @return save element number
   */
  public int getElementNum() {
    return elementNum;
  }

  /**
   * Set save element number
   *
   * @param elementNum save element number
   */
  public void setElementNum(int elementNum) {
    this.elementNum = elementNum;
  }

  /**
   * Get save type
   *
   * @return save type
   */
  public int getSaveType() {
    return saveType;
  }

  /**
   * Set save type
   *
   * @param saveType save type
   */
  public void setSaveType(int saveType) {
    this.saveType = saveType;
  }


  @Override public String toString() {
    return "RowPartitionMeta{" + "rowId=" + rowId + ", offset=" + offset + ", elementNum="
      + elementNum + '}';
  }
}
