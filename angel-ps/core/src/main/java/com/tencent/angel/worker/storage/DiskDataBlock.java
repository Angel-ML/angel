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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.utils.DataBlock;
import com.tencent.angel.worker.WorkerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The type Disk storage,stored value on disk
 * <p>
 * Use {@link Kryo} as serialization framework. <br>
 * Use {@link LocalDirAllocator} allocation for creating files
 * </p>
 *
 * @param <VALUE> the value type
 */
public class DiskDataBlock<VALUE> extends DataBlock<VALUE> {
  private static final Log LOG = LogFactory.getLog(DiskDataBlock.class);
  private final int taskIndex;
  private final ArrayList<String> filelist;
  private final LocalDirAllocator lDirAlloc;
  private final String base;

  private int currentReadFileIndex = 0; // current file index in kvFilelist
  private final int readBufferSize;
  // private KEY currentKey;
  private VALUE currentValue;
  private final Kryo kryo;
  private Input kryoInput;

  private final int writeBufferSize; // kvWriteBuffer size
  private long bytesWrittenToCurrentFile; // already written bytes to current file
  private final long maxSizePerFile; // max filesize, if current file is over this size, a new file
  // will be created
  private String currentWriteFileName; // current writing file name
  private int currentWriteFileIndex; // current writing file index
  private Output kryoOutput;
  private long writtenBytesInCompletedFile; // total write size for all completed file
  private final UUID uuid;

  public DiskDataBlock(int taskIndex) {
    super();
    this.taskIndex = taskIndex;

    filelist = new ArrayList<String>();

    lDirAlloc = new LocalDirAllocator(AngelConf.LOCAL_DIR);

    uuid = UUID.randomUUID();
    base = ContainerLocalizer.USERCACHE + "/" + WorkerContext.get().getUser() + "/"
      + ContainerLocalizer.APPCACHE + "/" + ConverterUtils.toString(WorkerContext.get().getAppId())
      + "/hdfsdatacache" + "/" + uuid;

    Configuration conf = WorkerContext.get().getConf();

    readBufferSize = conf.getInt(AngelConf.ANGEL_TASK_DISK_READ_BUFFER_SIZE,
      AngelConf.DEFAULT_ANGEL_TASK_DISK_READ_BUFFER_SIZE);

    writeBufferSize = conf.getInt(AngelConf.ANGEL_TASK_DISK_WRITE_BUFFER_SIZE,
      AngelConf.DEFAULT_ANGEL_TASK_DISK_WRITE_BUFFER_SIZE);

    maxSizePerFile = conf.getInt(AngelConf.ANGEL_TASK_RECORD_FILE_MAXSIZE_MB,
      AngelConf.DEFAULT_ANGEL_TASK_RECORD_FILE_MAXSIZE_MB) << 20;
    kryo = new Kryo();
    init();
    LOG.info("create diskstorage, base=" + base);
  }

  private void init() {
    bytesWrittenToCurrentFile = 0;
    currentWriteFileName = null;
    currentWriteFileIndex = 0;
    kryoOutput = null;
    kryoInput = null;
    writtenBytesInCompletedFile = 0;
    currentReadFileIndex = 0;
    currentValue = null;
  }

  @Override public void registerType(Class<VALUE> valueClass) {
    if (this.valueClass != null || valueClass == null) {
      return;
    }

    this.valueClass = valueClass;
    kryo.register(valueClass);
  }

  @Override public VALUE read() throws IOException {
    if (hasNext()) {
      readIndex++;
      currentValue = kryo.readObjectOrNull(kryoInput, valueClass);
      return currentValue;
    } else {
      return null;
    }
  }

  @Override protected boolean hasNext() throws IOException {
    if (kryoInput == null) {
      if (currentReadFileIndex >= filelist.size()) {
        return false;
      }
      FileInputStream currentInputStream = new FileInputStream(filelist.get(currentReadFileIndex));
      kryoInput = new Input(currentInputStream, readBufferSize);
    }

    if (!kryoInput.eof()) {
      return true;
    } else {
      LOG.debug("readerindex = " + readIndex);
      if (currentReadFileIndex == filelist.size() - 1) {
        return false;
      }
      shiftToNextFile();
      return true;
    }
  }

  private void shiftToNextFile() throws IOException {
    if (currentReadFileIndex < filelist.size() - 1) {
      currentReadFileIndex++;
      LOG.info("shift to read next file: " + filelist.get(currentReadFileIndex));
      FileInputStream currentInputStream = new FileInputStream(filelist.get(currentReadFileIndex));
      kryoInput.close();
      kryoInput.setInputStream(currentInputStream);
    } else {
      kryoInput.close();
    }
  }

  @Override public VALUE get(int index) throws IOException {
    throw new IOException("unsupport operation for " + this.getClass().getName());
  }

  @SuppressWarnings("unchecked") @Override public void put(VALUE value) throws IOException {
    if (value == null) {
      return;
    }
    registerType((Class<VALUE>) value.getClass());

    if (kryoOutput == null) {
      switchNextFile();
    }
    kryo.writeObjectOrNull(kryoOutput, value, valueClass);
    bytesWrittenToCurrentFile = kryoOutput.total();
    if (bytesWrittenToCurrentFile > maxSizePerFile) {
      switchNextFile();
    }
    writeIndex++;
  }

  private void switchNextFile() throws AngelException {
    OutputStream curOutputStream = null;
    if (kryoOutput != null) {
      writtenBytesInCompletedFile += kryoOutput.total();
      kryoOutput.close();
      bytesWrittenToCurrentFile = 0;
    }

    try {
      currentWriteFileName = getNextFileName();
      File file = new File(currentWriteFileName);
      assert (!file.exists());
      file = null;

      curOutputStream = new FileOutputStream(currentWriteFileName, false);
      if (kryoOutput != null) {
        kryoOutput.setOutputStream(curOutputStream);
      } else {
        kryoOutput = new Output(curOutputStream, writeBufferSize); // conf)
      }
    } catch (IOException ioe) {
      LOG.error("Switch to next file Error.", ioe);
      throw new AngelException(ioe);
    }
    filelist.add(currentWriteFileName);
    LOG.debug("add file " + currentWriteFileName + " to filelist");
  }

  private String getNextFileName() throws IOException {
    try {
      String filePath =
        base + "_" + WorkerContext.get().getWorkerAttemptId() + "_" + taskIndex + "_"
          + currentWriteFileIndex++;

      Path newPath =
        lDirAlloc.getLocalPathForWrite(filePath, maxSizePerFile, WorkerContext.get().getConf());

      LOG.info("KVDiskStorage create a new file, filePath = " + newPath);
      return newPath.toUri().toString();
    } catch (Exception e) {
      throw new IOException("task UnInitializtionException", e);
    }
  }

  @Override public void resetReadIndex() throws IOException {
    readIndex = 0;
    currentReadFileIndex = 0;
    if (filelist.size() > 0) {
      FileInputStream currentInputStream = new FileInputStream(filelist.get(currentReadFileIndex));
      kryoInput.close();
      kryoInput.setInputStream(currentInputStream);
    }
  }

  @Override public void clean() throws IOException {
    for (String path : filelist) {
      new File(path).deleteOnExit();
    }
    filelist.clear();
    init();
  }

  @Override public void shuffle() throws IOException {
    throw new IOException("no support operation for " + this.getClass().getName());
  }

  @Override public DataBlock<VALUE> slice(int startIndex, int length) throws IOException {
    throw new IOException("no support operation for " + this.getClass().getName());
  }

  @Override public void flush() throws IOException {
    if (kryoOutput != null) {
      kryoOutput.flush();
    }
  }

  @Override public String toString() {
    return "DiskDataBlock [super.toString=" + super.toString() + ", taskIndex=" + taskIndex
      + ", filelist=" + listToString(filelist) + ", base=" + base + ", currentReadFileIndex="
      + currentReadFileIndex + ", readBufferSize=" + readBufferSize + ", writeBufferSize="
      + writeBufferSize + ", bytesWrittenToCurrentFile=" + bytesWrittenToCurrentFile
      + ", maxSizePerFile=" + maxSizePerFile + ", currentWriteFileName=" + currentWriteFileName
      + ", currentWriteFileIndex=" + currentWriteFileIndex + ", writtenBytesInCompletedFile="
      + writtenBytesInCompletedFile + ", uuid=" + uuid + "]";
  }

  private String listToString(List<String> list) {
    StringBuilder sb = new StringBuilder();
    for (String s : list) {
      sb.append(s);
      sb.append(",");
    }

    return sb.toString();
  }
}
