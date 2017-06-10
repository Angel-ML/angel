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
import com.tencent.angel.worker.task.MRTaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * use new mr2 api
 *
 * @param <KEY> key type
 * @param <VALUE> value type
 */
public class DFSStorageNewAPI<KEY, VALUE> {
  private static final Log LOG = LogFactory.getLog(DFSStorageNewAPI.class);
  private final InputSplit split;
  private Reader<KEY, VALUE> reader;

  public DFSStorageNewAPI(InputSplit split) {
    this.split = split;
  }

  public InputSplit getSplit() {
    return split;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void initReader() throws IOException {
    try {
      Configuration conf = WorkerContext.get().getConf();
      String inputFormatClassName =
          conf.get(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS,
              AngelConfiguration.DEFAULT_ANGEL_INPUTFORMAT_CLASS);

      Class<? extends org.apache.hadoop.mapreduce.InputFormat> inputFormatClass =
          (Class<? extends org.apache.hadoop.mapreduce.InputFormat>) Class
              .forName(inputFormatClassName);

      org.apache.hadoop.mapreduce.InputFormat inputFormat =
          ReflectionUtils.newInstance(inputFormatClass,
              new JobConf(conf));

      MRTaskContext taskContext = new MRTaskContext(conf);
      org.apache.hadoop.mapreduce.RecordReader<KEY, VALUE> recordReader =
          inputFormat.createRecordReader(split, taskContext);

      recordReader.initialize(split, taskContext);
      setReader(new DFSReaderNewAPI(recordReader));
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

  public class DFSReaderNewAPI implements Reader<KEY, VALUE> {
    private org.apache.hadoop.mapreduce.RecordReader<KEY, VALUE> innerReader;

    public DFSReaderNewAPI(RecordReader<KEY, VALUE> reader) throws IOException {
      this.innerReader = reader;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return innerReader.nextKeyValue();
    }

    @Override
    public KEY getCurrentKey() throws IOException, InterruptedException {
      return innerReader.getCurrentKey();
    }

    @Override
    public VALUE getCurrentValue() throws IOException, InterruptedException {
      return innerReader.getCurrentValue();
    }

    @Override
    public void reset() throws IOException {
      innerReader.close();
      initReader();
    }

    @Override
    public void close() throws IOException {
      innerReader.close();
    }

    @Override
    public float getProgress() {
      try {
        return innerReader.getProgress();
      } catch (IOException | InterruptedException e) {
        LOG.error("reader get progress error", e);
        return 0.0f;
      }
    }
  }
}
