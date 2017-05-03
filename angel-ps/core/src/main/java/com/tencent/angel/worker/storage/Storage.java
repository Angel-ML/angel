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
 */

package com.tencent.angel.worker.storage;

import java.io.IOException;

/**
 * The type Storage.
 *
 * @param <VALUE> the value type
 */
public abstract class Storage<VALUE> {
  /**
   * The Value class.
   */
  protected Class<VALUE> valueClass;

  /**
   * The Read index.
   */
  protected int readIndex;

  /**
   * The Write index.
   */
  protected int writeIndex;

  public Storage() {
    readIndex = 0;
    writeIndex = 0;
  }

  /**
   * Register value type for serialization
   *
   * @param valueClass the value class
   */
  public void registerType(Class<VALUE> valueClass) {
    this.valueClass = valueClass;
  }

  /**
   * Read value sequentially
   *
   * @return the value
   * @throws IOException
   */
  public abstract VALUE read() throws IOException;

  /**
   * Has next value to read
   *
   * @return return true if has next value,else false
   * @throws IOException
   */
  protected abstract boolean hasNext() throws IOException;

  /**
   * Get value of index
   *
   * @param index the index
   * @return the value
   * @throws IOException
   */
  public abstract VALUE get(int index) throws IOException;

  /**
   * Put value sequentially
   *
   * @param value the value
   * @throws IOException
   */
  public abstract void put(VALUE value) throws IOException;

  /**
   * Reset read index at start position
   *
   * @throws IOException
   */
  public abstract void resetReadIndex() throws IOException;

  /**
   * Clean the values
   *
   * @throws IOException
   */
  public abstract void clean() throws IOException;

  /**
   * Shuffle the values
   * <p>
   * If unsupported,will throw IOException
   * </p>
   *
   * @throws IOException
   */
  public abstract void shuffle() throws IOException;

  /**
   * Flush the values,and forces any buffered write out
   *
   * @throws IOException
   */
  public abstract void flush() throws IOException;

  /**
   * Slice storage
   *
   * @param startIndex the start index
   * @param length the length
   * @return the sliced storage
   * @throws IOException the io exception
   */
  public abstract Storage<VALUE> slice(int startIndex, int length) throws IOException;

  public Class<VALUE> getValueClass() {
    return valueClass;
  }

  public void setValueClass(Class<VALUE> valueClass) {
    this.valueClass = valueClass;
  }

  public int getReadIndex() {
    return readIndex;
  }

  public void setReadIndex(int readIndex) {
    this.readIndex = readIndex;
  }

  public int getWriteIndex() {
    return writeIndex;
  }

  public void setWriteIndex(int writeIndex) {
    this.writeIndex = writeIndex;
  }

  /**
   * Gets total values number.
   *
   * @return the total elem num
   */
  public int getTotalElemNum() {
    return writeIndex;
  }

  /**
   * Gets reading progress
   * <p>
   * The default is read num/total num
   * </p>
   *
   * @return the progress
   */
  public float getProgress() {
    if (writeIndex == 0) {
      return 0.0f;
    }
    return (float) readIndex / writeIndex;
  }

  @Override
  public String toString() {
    return "Storage [valueClass=" + valueClass + ", readIndex=" + readIndex + ", writeIndex="
        + writeIndex + "]";
  }
}
