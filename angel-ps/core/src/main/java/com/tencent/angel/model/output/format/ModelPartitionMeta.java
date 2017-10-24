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

package com.tencent.angel.model.output.format;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The meta data for a Matrix partition.
 */
public class ModelPartitionMeta {
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
   * Create a empty PartitionMeta
   */
  public ModelPartitionMeta() {
    this(-1, -1, -1, -1, -1, 0, "", 0, 0);
  }

  /**
   * Create a PartitionMeta
   * @param partId partition index
   * @param startRow partition start row index
   * @param endRow partition end row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   * @param nnz Non-zero element number in this partition
   * @param fileName The file name to which the partition is written
   * @param offset The start position for this partition in the file
   * @param length total write bytes
   */
  public ModelPartitionMeta(int partId, int startRow, int endRow, long startCol, long endCol, long nnz, String fileName, long offset, long length) {
    this.partId = partId;
    this.startRow = startRow;
    this.endRow = endRow;
    this.startCol = startCol;
    this.endCol = endCol;
    this.nnz = nnz;
    this.fileName = fileName;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Write the partition meta to the stream
   * @param output output stream
   * @throws IOException
   */
  public void write(DataOutputStream output) throws IOException {
    output.writeInt(partId);
    output.writeInt(startRow);
    output.writeInt(endRow);
    output.writeLong(startCol);
    output.writeLong(endCol);
    output.writeLong(nnz);
    output.writeUTF(fileName);
    output.writeLong(offset);
    output.writeLong(length);
  }

  /**
   * Read partition meta from input stream
   * @param input
   * @throws IOException
   */
  public void read(DataInputStream input) throws IOException {
    partId = input.readInt();
    startRow = input.readInt();
    endRow = input.readInt();
    startCol = input.readLong();
    endCol = input.readLong();
    nnz = input.readLong();
    fileName = input.readUTF();
    offset = input.readLong();
    length = input.readLong();
  }

  /**
   * Get partition id
   * @return partition id
   */
  public int getPartId() {
    return partId;
  }

  /**
   * Set partition id
   * @param partId partition id
   */
  public void setPartId(int partId) {
    this.partId = partId;
  }

  /**
   * Get start row index of partition
   * @return start row index of partition
   */
  public int getStartRow() {
    return startRow;
  }

  /**
   * Set start row index of partition
   * @param startRow start row index of partition
   */
  public void setStartRow(int startRow) {
    this.startRow = startRow;
  }

  /**
   * Get end row index of partition
   * @return end row index of partition
   */
  public int getEndRow() {
    return endRow;
  }

  /**
   * Set end row index of partition
   * @param endRow end row index of partition
   */
  public void setEndRow(int endRow) {
    this.endRow = endRow;
  }

  /**
   * Get start column index of partition
   * @return start column index of partition
   */
  public long getStartCol() {
    return startCol;
  }

  /**
   * Set start column index of partition
   * @param startCol start column index of partition
   */
  public void setStartCol(long startCol) {
    this.startCol = startCol;
  }

  /**
   * Get end column index of partition
   * @return end column index of partition
   */
  public long getEndCol() {
    return endCol;
  }

  /**
   * Set end column index of partition
   * @param endCol end column index of partition
   */
  public void setEndCol(long endCol) {
    this.endCol = endCol;
  }

  /**
   * Get non-zero element number of partition
   * @return non-zero element number of partition
   */
  public long getNnz() {
    return nnz;
  }

  /**
   * Set non-zero element number of partition
   * @param nnz non-zero element number of partition
   */
  public void setNnz(int nnz) {
    this.nnz = nnz;
  }

  /**
   * Get the name of file which the partition is written to
   * @return file name
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Set the name of file which the partition is written to
   * @param fileName file name
   */
  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  /**
   * Get the start position of partition in file
   * @return start position
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Set the start position of partition in file
   * @param offset start position
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set non-zero element number
   * @param nnz
   */
  public void setNnz(long nnz) {
    this.nnz = nnz;
  }

  /**
   * Get written len
   * @return Written length
   */
  public long getLength() {
    return length;
  }

  /**
   * Set Written length
   * @param length Written length
   */
  public void setLength(long length) {
    this.length = length;
  }

  @Override public String toString() {
    return "PartitionMeta{" + "partId=" + partId + ", startRow=" + startRow + ", endRow=" + endRow
      + ", startCol=" + startCol + ", endCol=" + endCol + ", nnz=" + nnz + ", fileName='" + fileName
      + '\'' + ", offset=" + offset + ", length=" + length + '}';
  }
}
