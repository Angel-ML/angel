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
 *
 */

package com.tencent.angel.master.metrics;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The tool that write algorithm logs to file.
 */
public class DistributeLog {
  static final Log LOG = LogFactory.getLog(DistributeLog.class);

  /** Application configuration */
  private final Configuration conf;

  /** Index name list */
  private List<String> names;

  /** File output stream */
  private FSDataOutputStream outputStream;

  /**
   * Create a new DistributeLog
   * @param conf application configuration
   */
  public DistributeLog(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Init
   * @throws IOException
   */
  public void init() throws IOException {
    int flushLen = conf.getInt(AngelConf.ANGEL_LOG_FLUSH_MIN_SIZE, AngelConf.DEFAULT_ANGEL_LOG_FLUSH_MIN_SIZE);
    conf.setInt(DFSConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, flushLen);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, flushLen);

    String pathStr = conf.get(AngelConf.ANGEL_LOG_PATH);
    if (pathStr == null) {
      throw new IOException("log directory is null. you must set " + AngelConf.ANGEL_LOG_PATH);
    }

    LOG.info("algorithm log output directory=" + pathStr);

    Path path = new Path(pathStr + "/log");
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, true);
    }
    outputStream =  fs.create(path, true);
  }

  /**
   * Set the index name list
   * @param names index name list
   * @throws IOException
   */
  public void setNames(List<String> names) throws IOException {
    this.names = names;
  }

  /**
   * Write the index names to file
   * @throws IOException
   */
  public void writeNames() throws IOException {
    for (String name : names) {
      outputStream.write((name + "\t").getBytes());
    }
    outputStream.writeBytes("\n");
    outputStream.hflush();
  }

  /**
   * Write index values to file
   * @param algoIndexes index name to value map
   * @throws IOException
   */
  public void writeLog(Map<String, String> algoIndexes) throws IOException {
    assert(names != null && names.size() == algoIndexes.size());
    int size = names.size();
    for(int i = 0; i < size; i++) {
      outputStream.write((algoIndexes.get(names.get(i)) + "\t").getBytes());
    }
    outputStream.writeBytes("\n");
    outputStream.hflush();
  }

  /**
   * Close file writter
   * @throws IOException
   */
  public void close() throws IOException {
    if(outputStream != null) {
      outputStream.close();
    }
  }
}
