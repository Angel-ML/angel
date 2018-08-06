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

package com.tencent.angel.worker.storage;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.utils.MemoryUtils;
import com.tencent.angel.worker.WorkerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * The type Memory storage,stored value on array
 * <p>
 * Use {@link ArrayList} to hold values. <br>
 * Will estimate the value of memory occupation to calculate suitable array size when reached simple
 * number, make sure the memory will not over-limit
 * </p>
 * 
 * @param <VALUE> the type parameter
 */
public class MemoryDataBlock<VALUE> extends DataBlock<VALUE> {
  private static final Log LOG = LogFactory.getLog(MemoryDataBlock.class);
  private final ArrayList<VALUE> vList;
  private final long maxUseMemroy;
  private final int estimateSampleNumber;
  private long estimatedSize;
  private int maxStoreNum;
  private int startPos;

  public MemoryDataBlock(int initSize) {
    super();

    Configuration conf = WorkerContext.get().getConf();
    maxUseMemroy =
        conf.getLong(AngelConf.ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB,
            AngelConf.DEFAULT_ANGEL_TASK_MEMORYSTORAGE_USE_MAX_MEMORY_MB) * 1024 * 1024;

    estimateSampleNumber =
        conf.getInt(AngelConf.ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER,
            AngelConf.DEFAULT_ANGEL_TASK_ESTIMIZE_SAMPLE_NUMBER);

    int size = (initSize > 0) ? initSize : estimateSampleNumber;

    vList = new ArrayList<VALUE>(size);
    estimatedSize = -1;
    maxStoreNum = -1;
    startPos = -1;
  }

  public MemoryDataBlock(MemoryDataBlock<VALUE> other) {
    maxUseMemroy = other.getMaxUseMemroy();
    estimateSampleNumber = other.getEstimateSampleNumber();
    vList = other.getvList();
  }

  @Override
  public VALUE read() throws IOException {
    if (readIndex < writeIndex) {
      return vList.get(readIndex++);
    } else {
      return null;
    }
  }

  @Override
  protected boolean hasNext() throws IOException {
    return readIndex < writeIndex;
  }

  @Override
  public void put(VALUE value) throws IOException {
    vList.add(value);
    writeIndex++;

    if (estimatedSize == -1 && writeIndex == estimateSampleNumber) {
      estimateAndResizeVList();
    }
  }

  @Override
  public void resetReadIndex() throws IOException {
    readIndex = (startPos > 0) ? startPos : 0;
  }

  @Override
  public void clean() throws IOException {
    readIndex = 0;
    writeIndex = 0;
    vList.clear();
  }

  @Override
  public void shuffle() {
    Collections.shuffle(vList);
  }

  @Override
  public DataBlock<VALUE> slice(int startIndex, int length) {
    MemoryDataBlock<VALUE> other = new MemoryDataBlock<VALUE>(this);
    other.startPos = startIndex;
    other.setReadIndex(startIndex);
    other.setWriteIndex(length + startIndex);
    return other;
  }

  public ArrayList<VALUE> getvList() {
    return vList;
  }

  public long getMaxUseMemroy() {
    return maxUseMemroy;
  }

  public int getEstimateSampleNumber() {
    return estimateSampleNumber;
  }

  @Override
  public VALUE get(int index) throws IOException {
    if (index < 0 || index >= writeIndex) {
      throw new IOException("index not in range[0," + writeIndex + ")");
    }

    return vList.get(index);
  }

  @Override
  public void flush() throws IOException {

  }

  public void estimateAndResizeVList() {
    estimatedSize = MemoryUtils.estimateMemorySize(vList) / vList.size();
    maxStoreNum = (int) (maxUseMemroy / estimatedSize);

    vList.ensureCapacity(maxStoreNum);
    LOG.debug("estimate sample number=" + vList.size() + ", estimatedSize=" + estimatedSize
        + ", maxStoreNum=" + maxStoreNum + ", maxUseMemroy=" + maxUseMemroy);
  }

  public boolean checkIsOverMaxMemoryUsed() {
    return writeIndex >= maxStoreNum;
  }

  @Override
  public String toString() {
    return "MemoryDataBlock [vList size=" + vList.size() + ", maxUseMemroy=" + maxUseMemroy
        + ", estimateSampleNumber=" + estimateSampleNumber + ", estimatedSize=" + estimatedSize
        + ", maxStoreNum=" + maxStoreNum + ", toString()=" + super.toString() + "]";
  }
}
