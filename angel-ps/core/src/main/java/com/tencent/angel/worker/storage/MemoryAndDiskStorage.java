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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * The type Memory and Disk storage.
 * <p>
 * The values will store on memory is available,and then on disk
 * </p>
 *
 * @param <VALUE> the type parameter
 */
public class MemoryAndDiskStorage<VALUE> extends Storage<VALUE> {
  private static final Log LOG = LogFactory.getLog(MemoryAndDiskStorage.class);
  private MemoryStorage<VALUE> memoryStorage;
  private DiskStorage<VALUE> diskStorage;
  private boolean memoryWriteInUse;
  private boolean memoryReadInUse;
  private final int memoryCheckInterval;
  private int memoryCheckCounter;
  private final int taskIndex;

  public MemoryAndDiskStorage(int taskIndex) {
    super();
    this.taskIndex = taskIndex;
    memoryStorage = new MemoryStorage<VALUE>(-1);
    diskStorage = null;
    memoryReadInUse = true;
    memoryWriteInUse = true;
    memoryCheckInterval = 1000;
    memoryCheckCounter = 0;
  }

  @Override
  public VALUE read() throws IOException {
    VALUE value = null;
    readIndex++;
    if (memoryReadInUse) {
      value = memoryStorage.read();
      if (value != null) {
        return value;
      } else {
        if (diskStorage == null) {
          return null;
        }
        memoryReadInUse = false;
      }
    }

    return diskStorage.read();
  }

  @Override
  protected boolean hasNext() throws IOException {
    if (readIndex < memoryStorage.writeIndex) {
      return true;
    } else {
      if (diskStorage != null) {
        memoryReadInUse = false;
        return diskStorage.hasNext();
      } else {
        return false;
      }
    }
  }

  @Override
  public VALUE get(int index) throws IOException {
    if (index < memoryStorage.writeIndex) {
      return memoryStorage.get(index);
    } else {
      throw new IOException("dose not support this get random operation for DiskStorage");
    }
  }

  @Override
  public void put(VALUE value) throws IOException {
    if (memoryWriteInUse) {
      memoryStorage.put(value);
      writeIndex++;
      memoryCheckCounter++;
      if (memoryCheckCounter == memoryCheckInterval) {
        if (memoryStorage.checkIsOverMaxMemoryUsed()) {
          diskStorage = new DiskStorage<VALUE>(taskIndex);
          diskStorage.registerType(valueClass);
          memoryWriteInUse = false;
        }
        memoryCheckCounter = 0;
      }
    } else {
      diskStorage.put(value);
      writeIndex++;
    }
  }

  @Override
  public void resetReadIndex() throws IOException {
    readIndex = 0;
    if (memoryReadInUse) {
      memoryStorage.resetReadIndex();
    } else {
      memoryReadInUse = true;
      memoryStorage.resetReadIndex();
      diskStorage.resetReadIndex();
    }
  }

  @Override
  public void clean() throws IOException {
    memoryStorage.clean();
    if (diskStorage != null) {
      diskStorage.clean();
    }
    memoryReadInUse = true;
    memoryWriteInUse = true;
    memoryCheckCounter = 0;
    readIndex = writeIndex = 0;

  }

  @Override
  public void shuffle() throws IOException {
    memoryStorage.shuffle();
  }

  @Override
  public void flush() throws IOException {
    if (memoryWriteInUse) {
      memoryStorage.flush();
    } else {
      diskStorage.flush();
    }
  }

  @Override
  public Storage<VALUE> slice(int startIndex, int length) throws IOException {
    if (startIndex + length <= memoryStorage.writeIndex) {
      return memoryStorage.slice(startIndex, length);
    } else {
      throw new IOException("does not support this slice operation for DiskStorage");
    }
  }

  @Override
  public String toString() {
    return "MemoryAndDiskStorage [memoryStorage=" + memoryStorage + ", diskStorage=" + diskStorage
        + ", memoryWriteInUse=" + memoryWriteInUse + ", memoryReadInUse=" + memoryReadInUse
        + ", memoryCheckInterval=" + memoryCheckInterval + ", memoryCheckCounter="
        + memoryCheckCounter + ", taskIndex=" + taskIndex + ", toString()=" + super.toString()
        + "]";
  }
}
