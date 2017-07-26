package com.tencent.angel.master.metrics;

import com.tencent.angel.conf.AngelConf;
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
  private static final String LOG_FORMAT = "%10.6e";

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
   * Write the index names to file
   * @param names index name list
   * @throws IOException
   */
  public void setNames(List<String> names) throws IOException {
    this.names = names;
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
  public void writeLog(Map<String, Double> algoIndexes) throws IOException {
    assert(names != null && names.size() == algoIndexes.size());
    int size = names.size();
    for(int i = 0; i < size; i++) {
      outputStream.write(String.format(LOG_FORMAT + "\t", algoIndexes.get(names.get(i))).getBytes());
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
