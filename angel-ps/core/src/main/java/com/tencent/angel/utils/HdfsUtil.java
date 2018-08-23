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


package com.tencent.angel.utils;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.predict.PredictResult;
import com.tencent.angel.worker.storage.DataBlock;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * HDFS operation(like list,copy etcs) utils.
 */
public class HdfsUtil {
  private static final Log LOG = LogFactory.getLog(HdfsUtil.class);
  public static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
  public static final String SPLIT_MAXSIZE = "mapreduce.input.fileinputformat.split.maxsize";
  public static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
  public static final String PATHFILTER_CLASS = "mapreduce.input.pathFilter.class";
  public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";
  public static final String INPUT_DIR_RECURSIVE =
    "mapreduce.input.fileinputformat.input.dir.recursive";


  private static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }


  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };
  private static String tmpPrefix = "_tmp.";
  private static String finalPrefix = "_final.";
  private static String pathSep = "/";

  public static Path[] getInputPaths(JobContext context) {
    String dirs = context.getConfiguration().get(INPUT_DIR, "");
    // LOG.info(System.getProperty("user.dir"));
    LOG.info("dirs=" + dirs);
    String[] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  public static boolean getInputDirRecursive(JobContext job) {
    return job.getConfiguration().getBoolean(INPUT_DIR_RECURSIVE, false);
  }

  public static PathFilter getInputPathFilter(JobContext context) {
    Configuration conf = context.getConfiguration();
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null, PathFilter.class);
    return (filterClass != null) ?
      (PathFilter) ReflectionUtils.newInstance(filterClass, conf) :
      null;
  }

  /**
   * List input directories. Subclasses may override to, e.g., select only files matching a regular
   * expression.
   *
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected static List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

    // Whether we need to recursive look into the directory structure
    boolean recursive = getInputDirRecursive(job);

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i = 0; i < dirs.length; ++i) {
      LOG.info("dirs[" + i + "]=" + dirs[i]);
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job.getConfiguration());
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat : matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    // LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  protected static void addInputPathRecursively(List<FileStatus> result, FileSystem fs, Path path,
    PathFilter inputFilter) throws IOException {
    RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
    while (iter.hasNext()) {
      LocatedFileStatus stat = iter.next();
      if (inputFilter.accept(stat.getPath())) {
        if (stat.isDirectory()) {
          addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
        } else {
          result.add(stat);
        }
      }
    }
  }

  public static long getInputFileTotalSize(JobContext job) throws IOException {
    long ret = 0;
    LOG.info("before getInputFileTotalSize");
    List<FileStatus> fss = listStatus(job);
    if (fss != null) {
      for (FileStatus fs : fss) {
        ret += fs.getLen();
      }
    }
    return ret;
  }

  public static Path[] getInputPaths(JobConf context) {
    String dirs = context.get(INPUT_DIR, "");
    LOG.info("dirs=" + dirs);
    String[] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }

  public static boolean getInputDirRecursive(JobConf job) {
    return job.getBoolean(INPUT_DIR_RECURSIVE, false);
  }

  public static PathFilter getInputPathFilter(JobConf context) {
    Configuration conf = context;
    Class<?> filterClass = conf.getClass(PATHFILTER_CLASS, null, PathFilter.class);
    return (filterClass != null) ?
      (PathFilter) ReflectionUtils.newInstance(filterClass, conf) :
      null;
  }

  /**
   * List input directories. Subclasses may override to, e.g., select only files matching a regular
   * expression.
   *
   * @param job the job to list input paths for
   * @return array of FileStatus objects
   * @throws IOException if zero items.
   */
  protected static List<FileStatus> listStatus(JobConf job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }

    // get tokens for all the required FileSystems..
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);

    // Whether we need to recursive look into the directory structure
    boolean recursive = getInputDirRecursive(job);

    List<IOException> errors = new ArrayList<IOException>();

    // creates a MultiPathFilter with the hiddenFileFilter and the
    // user provided one (if any).
    List<PathFilter> filters = new ArrayList<PathFilter>();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);

    for (int i = 0; i < dirs.length; ++i) {
      LOG.info("dirs[" + i + "]=" + dirs[i]);
      Path p = dirs[i];
      FileSystem fs = p.getFileSystem(job);
      FileStatus[] matches = fs.globStatus(p, inputFilter);
      if (matches == null) {
        errors.add(new IOException("Input path does not exist: " + p));
      } else if (matches.length == 0) {
        errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
      } else {
        for (FileStatus globStat : matches) {
          if (globStat.isDirectory()) {
            RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(globStat.getPath());
            while (iter.hasNext()) {
              LocatedFileStatus stat = iter.next();
              if (inputFilter.accept(stat.getPath())) {
                if (recursive && stat.isDirectory()) {
                  addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
                } else {
                  result.add(stat);
                }
              }
            }
          } else {
            result.add(globStat);
          }
        }
      }
    }

    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    // LOG.info("Total input paths to process : " + result.size());
    return result;
  }

  public static long getInputFileTotalSize(JobConf jobConf) throws IOException {
    long ret = 0;
    LOG.info("before getInputFileTotalSize");
    List<FileStatus> fss = listStatus(jobConf);
    if (fss != null) {
      for (FileStatus fs : fss) {
        ret += fs.getLen();
      }
    }
    return ret;
  }

  public static Path toTmpPath(Path path) {
    return new Path(path.getParent(), tmpPrefix + path.getName());
  }

  public static void copy(Path tmpDestFile, Path destFile) {

  }

  // a simple hdfs copy function assume src path and dest path are in same hdfs
  // and FileSystem object has same schema
  static public void copyFilesInSameHdfs(Path srcf, Path destf, FileSystem fs) throws IOException {
    if (!fs.exists(destf))
      fs.mkdirs(destf);

    FileStatus[] srcs = fs.globStatus(srcf);
    if (srcs == null) {
      return;
    }

    for (int i = 0; i < srcs.length; i++) {
      copyDir(srcs[i].getPath(), destf, fs);
    }
  }

  public static boolean isTmpPath(String name) {
    return name.startsWith(tmpPrefix);
  }

  @SuppressWarnings("deprecation") private static void copyDir(Path srcf, Path destf, FileSystem fs)
    throws IOException {
    FileStatus[] items = fs.listStatus(srcf);
    for (int i = 0; i < items.length; i++) {
      if (items[i].isDir()) {
        Path destPath = new Path(destf, items[i].getPath().getName());

        if (!fs.exists(destPath)) {
          fs.mkdirs(destPath);
        }
        copyDir(items[i].getPath(), destPath, fs);
      } else {
        if (isTmpPath(items[i].getPath().getName())) {
          continue;
        }
        if (!fs.rename(items[i].getPath(), new Path(destf, items[i].getPath().getName()))) {
          throw new IOException(
            "rename from " + items[i].getPath() + " to " + destf + "/" + items[i].getPath()
              .getName() + " failed");
        }
      }
    }
  }

  public static Path toFinalPath(Path path) {
    return new Path(path.getParent(), finalPrefix + path.getName());
  }

  public static void rename(Path tmpCombinePath, Path outputPath, FileSystem fs)
    throws IOException {
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    if (!fs.rename(tmpCombinePath, outputPath)) {
      throw new IOException("rename from " + tmpCombinePath + " to " + outputPath + " failed");
    }
  }

  public static Path generateTmpDirectory(Configuration conf, String appId, Path outputPath) {
    URI uri = outputPath.toUri();
    String path =
      (uri.getScheme() != null ? uri.getScheme() : "hdfs") + "://" + (uri.getHost() != null ?
        uri.getHost() :
        "") + (uri.getPort() > 0 ? (":" + uri.getPort()) : "");
    String user = conf.get(AngelConf.USER_NAME, "");
    String tmpDir = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH_PREFIX, "/tmp/" + user);
    String finalTmpDirForApp = path + tmpDir + "/" + appId + "_" + UUID.randomUUID().toString();
    LOG.info("tmp output dir is " + finalTmpDirForApp);
    return new Path(finalTmpDirForApp);
  }

  public static void writeStorage(DataBlock<PredictResult> dataBlock, TaskContext taskContext)
    throws IOException {
    String outDir = taskContext.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    Path outPath = new Path(outDir, "predict");
    FileSystem fs = outPath.getFileSystem(taskContext.getConf());
    String outFileName = "task_" + taskContext.getTaskIndex();
    String tmpOutFileName = tmpPrefix + outFileName;

    Path outFilePath = new Path(outPath, outFileName);
    Path tmpOutFilePath = new Path(outPath, tmpOutFileName);
    if (fs.exists(tmpOutFilePath)) {
      fs.delete(tmpOutFilePath, true);
    }

    FSDataOutputStream output = fs.create(tmpOutFilePath);
    LOG.info("tmpOutFilePath=" + tmpOutFilePath);
    dataBlock.resetReadIndex();
    PredictResult resultItem = null;
    boolean isFirstRow = true;
    while (true) {
      resultItem = dataBlock.read();
      if (resultItem == null) {
        break;
      }

      output.writeBytes(resultItem.getText() + "\n");
    }

    output.close();

    rename(tmpOutFilePath, outFilePath, fs);
    LOG.info("rename " + tmpOutFilePath + " to " + outFilePath);
  }
}
