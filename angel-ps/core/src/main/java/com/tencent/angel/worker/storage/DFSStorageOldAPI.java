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

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.worker.WorkerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * use new mr1 api
 *
 * @param <KEY> key type
 * @param <VALUE> value type
 */
public class DFSStorageOldAPI<KEY, VALUE> {
  private static final Log LOG = LogFactory.getLog(DFSStorageOldAPI.class);
  private final InputSplit split;
  private Reader<KEY, VALUE> reader;

  public DFSStorageOldAPI(InputSplit split) {
    this.split = split;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void initReader() throws IOException {
    try {
      Configuration conf = WorkerContext.get().getConf();
      String inputFormatClassName =
          conf.get(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS,
              AngelConfiguration.DEFAULT_ANGEL_INPUTFORMAT_CLASS);

      Class<? extends org.apache.hadoop.mapred.InputFormat> inputFormatClass =
          (Class<? extends org.apache.hadoop.mapred.InputFormat>) Class
              .forName(inputFormatClassName);

      org.apache.hadoop.mapred.InputFormat inputFormat =
          (org.apache.hadoop.mapred.InputFormat) ReflectionUtils.newInstance(inputFormatClass,
              new JobConf(conf));

      org.apache.hadoop.mapred.RecordReader<KEY, VALUE> recordReader =
          inputFormat.getRecordReader(split, new JobConf(conf), null);

      setReader(new DFSReaderOldAPI(recordReader));
    } catch (Exception x) {
      LOG.error("init reader error ", x);
      throw new IOException(x);
    }
  }

  public Reader<KEY, VALUE> getReader() {
    return reader;
  }

  public void setReader(Reader<KEY, VALUE> reader) {
    this.reader = reader;
  }

  public class DFSReaderOldAPI implements Reader<KEY, VALUE> {
    private org.apache.hadoop.mapred.RecordReader<KEY, VALUE> innerReader;
    private KEY currentKey;
    private VALUE currentValue;

    public DFSReaderOldAPI(RecordReader<KEY, VALUE> reader) throws IOException {
      this.innerReader = reader;
    }

    @Override
    public void reset() throws IOException {
      close();
      currentKey = null;
      currentValue = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (currentKey == null) {
        currentKey = innerReader.createKey();
        currentValue = innerReader.createValue();
      }

      boolean hasNext = innerReader.next(currentKey, currentValue);
      return hasNext;
    }

    @Override
    public KEY getCurrentKey() throws IOException, InterruptedException {
      return currentKey;
    }

    @Override
    public VALUE getCurrentValue() throws IOException, InterruptedException {
      return currentValue;
    }

    @Override
    public void close() throws IOException {
      innerReader.close();
    }

    @Override
    public float getProgress() {
      try {
        return innerReader.getProgress();
      } catch (IOException e) {
        LOG.error("reader get progress error", e);
        return 0.0f;
      }
    }
  }
}
