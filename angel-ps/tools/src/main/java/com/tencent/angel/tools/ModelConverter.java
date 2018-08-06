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

package com.tencent.angel.tools;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.ModelFilesMeta;
import com.tencent.angel.model.output.format.ModelPartitionMeta;
import com.tencent.angel.utils.ConfUtils;
import com.tencent.angel.utils.UGITools;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Model convert tools, it convert model with angel format to other format
 */
public class ModelConverter {
  private static final Log LOG = LogFactory.getLog(ModelConverter.class);
  private static volatile int batchNum = 1;
  private final static ForkJoinPool pool =
    new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);

  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  /**
   * Convert a angel model to a text format
   * @param conf application configuration
   * @param modelInputDir the directory of angel model files
   * @param modelOutputDir the save directory of converted model files
   * @throws IOException
   */
  public static void convert(Configuration conf, String modelInputDir, String modelOutputDir)
    throws IOException {
    convert(conf, modelInputDir, modelOutputDir, new TextModelLineConvert());
  }


  /**
   * Convert a angel model to other format
   * @param conf application configuration
   * @param modelInputDir the directory of angel model files
   * @param modelOutputDir the save directory of converted model files
   * @param lineConvert format serializer
   * @throws IOException
   */
  public static void convert(Configuration conf, String modelInputDir, String modelOutputDir, ModelLineConvert lineConvert)
    throws IOException {
    // Load model meta
    ModelFilesMeta meta = getMeta(modelInputDir, conf);

    // Convert model
    convertModel(conf, modelInputDir, modelOutputDir, meta, lineConvert);
  }

  private static void convertModel(Configuration conf, String modelInputDir,
    String convertedModelDir, ModelFilesMeta meta, ModelLineConvert lineConvert)
    throws IOException {
    List<List<ModelPartitionMeta>> groupByParts = groupByPartitions(meta.getPartMetas());
    Path modelPath = new Path(modelInputDir);
    Path convertedModelPath = new Path(convertedModelDir);
    FileSystem modelFs = modelPath.getFileSystem(conf);
    FileSystem convertedModelFs = convertedModelPath.getFileSystem(conf);

    if(convertedModelFs.exists(convertedModelPath)) {
      LOG.warn("output directory " + convertedModelDir + " already exists");
      convertedModelFs.delete(convertedModelPath,true);
    }

    Vector<String> errorLogs = new Vector<>();
    ConvertOp convertOp =
      new ConvertOp(modelPath, modelFs, convertedModelPath, convertedModelFs, lineConvert, groupByParts, meta, errorLogs, 0, groupByParts.size());
    pool.execute(convertOp);
    convertOp.join();
    if(!errorLogs.isEmpty()) {
      throw new IOException(String.join("\n", errorLogs));
    }
  }

  private static List<List<ModelPartitionMeta>> groupByPartitions(
    Map<Integer, ModelPartitionMeta> partMetas) {
    List<List<ModelPartitionMeta>> ret = new ArrayList<>();
    HashMap<String, List<ModelPartitionMeta>> fileNameToPartsMap = new HashMap<>();
    for (ModelPartitionMeta partMeta : partMetas.values()) {
      List<ModelPartitionMeta> modelParts = fileNameToPartsMap.get(partMeta.getFileName());
      if (modelParts == null) {
        modelParts = new ArrayList<>();
        fileNameToPartsMap.put(partMeta.getFileName(), modelParts);
      }

      modelParts.add(partMeta);
    }

    for (List<ModelPartitionMeta> partList : fileNameToPartsMap.values()) {
      Collections.sort(partList, new Comparator<ModelPartitionMeta>() {
        @Override public int compare(ModelPartitionMeta part1, ModelPartitionMeta part2) {
          return (int) (part1.getOffset() - part2.getOffset());
        }
      });

      ret.add(partList);
    }

    return ret;
  }

  /**
   * Convert operation
   */
  static class ConvertOp extends RecursiveAction {
    /**
     * Angel model path
     */
    private final Path modelPath;

    /**
     * File system handler of model path
     */
    private final FileSystem modelFs;

    /**
     * Converted model save path
     */
    private final Path convertedModelPath;

    /**
     * Model line converter
     */
    private final ModelLineConvert lineConvert;

    /**
     * File system handler of Model path
     */
    private final FileSystem convertedModelFs;

    /**
     * Need load partitions list
     */
    private final List<List<ModelPartitionMeta>> groupByParts;
    /**
     * Model meta
     */
    private final ModelFilesMeta meta;
    /**
     * Error logs
     */
    private final Vector<String> errorMsgs;
    /**
     * ForkJoin start position
     */
    private final int startPos;
    /**
     * ForkJoin end position
     */
    private final int endPos;

    /**
     * Create a convert op
     * @param modelPath Angel model path
     * @param modelFs File system handler of model path
     * @param convertedModelPath converted model save path
     * @param convertedModelFs File system handler of converted model save path
     * @param lineConvert Serializer
     * @param groupByParts Partitions group by file
     * @param meta Model meta
     * @param errorMsgs Error logs
     * @param startPos ForkJoin start position
     * @param endPos ForkJoin end position
     */
    public ConvertOp(Path modelPath, FileSystem modelFs, Path convertedModelPath,
      FileSystem convertedModelFs, ModelLineConvert lineConvert,
      List<List<ModelPartitionMeta>> groupByParts, ModelFilesMeta meta, Vector<String> errorMsgs,
      int startPos, int endPos) {
      this.modelPath = modelPath;
      this.modelFs = modelFs;
      this.convertedModelPath = convertedModelPath;
      this.convertedModelFs = convertedModelFs;
      this.lineConvert = lineConvert;
      this.groupByParts = groupByParts;
      this.meta = meta;
      this.errorMsgs = errorMsgs;
      this.startPos = startPos;
      this.endPos = endPos;
    }

    @Override protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos <= 1) {
        try {
          convertPartitions(modelPath, modelFs, convertedModelPath, convertedModelFs, lineConvert,
            groupByParts.get(startPos), meta);
        } catch (Throwable x) {
          LOG.error("convert model partitions failed.", x);
          errorMsgs.add("convert model partitions failed." + x.getMessage());
        }
      } else {
        int middle = (startPos + endPos) / 2;
        ConvertOp opLeft =
          new ConvertOp(modelPath, modelFs, convertedModelPath, convertedModelFs, lineConvert,
            groupByParts, meta, errorMsgs, startPos, middle);
        ConvertOp opRight =
          new ConvertOp(modelPath, modelFs, convertedModelPath, convertedModelFs, lineConvert,
            groupByParts, meta, errorMsgs, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private static void convertPartitions(Path modelPath, FileSystem modelFs, Path convertedModelPath,
    FileSystem convertedModelFs, ModelLineConvert lineConvert, List<ModelPartitionMeta> partMetas,
    ModelFilesMeta modelMeta) throws IOException {
    if (partMetas == null || partMetas.isEmpty()) {
      return;
    }

    String fileName = partMetas.get(0).getFileName();
    int size = partMetas.size();
    LOG.info("start to convert partitions in file " + fileName + ", partition number is " + size);

    long offset = 0;
    FSDataInputStream input = modelFs.open(new Path(modelPath, fileName));
    FSDataOutputStream output = convertedModelFs.create(new Path(convertedModelPath, fileName));

    for (int i = 0; i < size; i++) {
      ModelPartitionMeta partMeta = modelMeta.getPartMeta(partMetas.get(i).getPartId());
      offset = partMeta.getOffset();
      input.seek(offset);
      convertPartition(input, output, lineConvert, partMeta, modelMeta);
    }

    input.close();
    output.close();
  }

  private static void convertPartition(FSDataInputStream input, FSDataOutputStream output,
    ModelLineConvert lineConvert, ModelPartitionMeta partMeta, ModelFilesMeta modelMeta)
    throws IOException {
    RowType rowType = RowType.valueOf(modelMeta.getRowType());
    switch (rowType) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        convertSparseDoublePartition(input, output, lineConvert, partMeta);
        break;

      case T_DOUBLE_DENSE:
        convertDenseDoublePartition(input, output, lineConvert, partMeta);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        convertSparseIntPartition(input, output, lineConvert, partMeta);
        break;

      case T_INT_DENSE:
        convertDenseIntPartition(input, output, lineConvert, partMeta);
        break;

      case T_FLOAT_DENSE:
        convertDenseFloatPartition(input, output, lineConvert, partMeta);
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        convertSparseFloatPartition(input, output, lineConvert, partMeta);
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        convertSparseDoubleLongKeyPartition(input, output, lineConvert, partMeta);
        break;

      default:
        throw new IOException("unsupport row type");
    }
  }

  private static void convertSparseDoublePartition(FSDataInputStream input,
    FSDataOutputStream output, ModelLineConvert lineConvert, ModelPartitionMeta partMeta)
    throws IOException {
    int rowNum = input.readInt();
    int nnz = 0;
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      nnz = input.readInt();
      for (int j = 0; j < nnz; j++) {
        lineConvert.convertDouble(output, input.readInt(), input.readDouble());
      }
    }
  }

  private static void convertDenseDoublePartition(FSDataInputStream input,
    FSDataOutputStream output, ModelLineConvert lineConvert, ModelPartitionMeta partMeta)
    throws IOException {
    int rowNum = input.readInt();
    LOG.info("start to convert partition " + partMeta);
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      for (int j = startCol; j < endCol; j++) {
        lineConvert.convertDouble(output, j, input.readDouble());
      }
    }
  }

  private static void convertSparseFloatPartition(FSDataInputStream input,
    FSDataOutputStream output, ModelLineConvert lineConvert, ModelPartitionMeta partMeta)
    throws IOException {
    int rowNum = input.readInt();
    int nnz = 0;
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      nnz = input.readInt();
      for (int j = 0; j < nnz; j++) {
        lineConvert.convertFloat(output, input.readInt(), input.readFloat());
      }
    }
  }

  private static void convertDenseFloatPartition(FSDataInputStream input, FSDataOutputStream output,
    ModelLineConvert lineConvert, ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      for (int j = startCol; j < endCol; j++) {
        lineConvert.convertFloat(output, j, input.readFloat());
      }
    }
  }

  private static void convertSparseIntPartition(FSDataInputStream input, FSDataOutputStream output,
    ModelLineConvert lineConvert, ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int nnz = 0;
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      nnz = input.readInt();
      for (int j = 0; j < nnz; j++) {
        lineConvert.convertInt(output, input.readInt(), input.readInt());
      }
    }
  }

  private static void convertDenseIntPartition(FSDataInputStream input, FSDataOutputStream output,
    ModelLineConvert lineConvert, ModelPartitionMeta partMeta) throws IOException {
    int rowNum = input.readInt();
    int startCol = (int) partMeta.getStartCol();
    int endCol = (int) partMeta.getEndCol();
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      for (int j = startCol; j < endCol; j++) {
        lineConvert.convertInt(output, j, input.readInt());
      }
    }
  }

  private static void convertSparseDoubleLongKeyPartition(FSDataInputStream input,
    FSDataOutputStream output, ModelLineConvert lineConvert, ModelPartitionMeta partMeta)
    throws IOException {
    int rowNum = input.readInt();
    int nnz = 0;
    for (int i = 0; i < rowNum; i++) {
      lineConvert.convertRowIndex(output, input.readInt());
      nnz = input.readInt();
      for (int j = 0; j < nnz; j++) {
        lineConvert.convertDoubleLongKey(output, input.readLong(), input.readDouble());
      }
    }
  }

  /**
   * Get model meta
   *
   * @param modelDir model save directory path
   * @return model meta
   * @throws IOException
   */
  public static ModelFilesMeta getMeta(String modelDir, Configuration conf) throws IOException {
    Path modelPath = new Path(modelDir);
    Path meteFilePath = new Path(modelPath, ModelFilesConstent.modelMetaFileName);
    ModelFilesMeta meta = new ModelFilesMeta();
    FileSystem fs = meteFilePath.getFileSystem(conf);
    if (!fs.exists(meteFilePath)) {
      throw new IOException("matrix meta file does not exist ");
    }
    FSDataInputStream input = fs.open(meteFilePath);
    meta.read(input);
    input.close();
    return meta;
  }

  public static void main(String [] args) throws IOException {
    /*final Configuration conf = new Configuration();
    // load hadoop configuration
    String hadoopHomePath = System.getenv("HADOOP_HOME");
    if (hadoopHomePath == null) {
      LOG.warn("HADOOP_HOME is empty.");
    } else {
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/yarn-site.xml"));
      conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/hdfs-site.xml"));
    }

    String baseDir = LOCAL_FS + TMP_PATH + "/out";
    String denseDoubleDir = baseDir + "/dense_double";
    String convertedDenseDoubleDir = baseDir + "/converted_dense_double";
    convert(conf, denseDoubleDir, convertedDenseDoubleDir);
    String sparseDoubleDir = baseDir + "/sparse_double";
    String convertedSparseDoubleDir = baseDir + "/converted_sparse_double";
    convert(conf, sparseDoubleDir, convertedSparseDoubleDir);
    String denseFloatDir = baseDir + "/dense_float";
    String convertedDenseFloatDir = baseDir + "/converted_dense_float";
    convert(conf, denseFloatDir, convertedDenseFloatDir);
    String sparseFloatDir = baseDir + "/sparse_float";
    String convertedSparseFloatDir = baseDir + "/converted_sparse_float";
    convert(conf, sparseFloatDir, convertedSparseFloatDir);
    String denseIntDir = baseDir + "/dense_int";
    String convertedDenseIntDir = baseDir + "/converted_dense_int";
    convert(conf, denseIntDir, convertedDenseIntDir);
    String sparseIntDir = baseDir + "/sparse_int";
    String convertedSparseIntDir = baseDir + "/converted_sparse_int";
    convert(conf, sparseIntDir, convertedSparseIntDir);
    String sparseDoubleLongKeyDir = baseDir + "/sparse_double_longkey";
    String convertedSparseDoubleLongKeyDir = baseDir + "/converted_sparse_double_longkey";
    convert(conf, sparseDoubleLongKeyDir, convertedSparseDoubleLongKeyDir);
    */

    try {
      final Configuration conf = ConfUtils.initConf(args);
      UserGroupInformation ugi = UGITools.getCurrentUser(conf);
      ugi.doAs(new PrivilegedExceptionAction<String>() {
        @Override public String run() throws Exception {
          // Get input path, output path
          String modelLoadDir= conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH);
          if(modelLoadDir == null) {
            LOG.fatal("convert source path " + AngelConf.ANGEL_LOAD_MODEL_PATH + " must be set");
            return "FAILED";
          }
          String convertedModelSaveDir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH);
          if(convertedModelSaveDir == null) {
            LOG.fatal("converted model save path " + AngelConf.ANGEL_LOAD_MODEL_PATH + " must be set");
            return "FAILED";
          }

          // Init serde
          String modelSerdeClass = conf.get("angel.modelconverts.serde.class", TextModelLineConvert.class.getName());
          Class<? extends ModelLineConvert> funcClass =
            (Class<? extends ModelLineConvert>) Class.forName(modelSerdeClass);
          Constructor<? extends ModelLineConvert> constructor = funcClass.getConstructor();
          constructor.setAccessible(true);
          ModelLineConvert serde = constructor.newInstance();

          // Parse need convert model names, if not set, we will convert all models in input directory
          String needConvertModelNames = conf.get("angel.modelconverts.model.names");
          String [] modelNames = null;
          if(needConvertModelNames == null) {
            LOG.info("we will convert all models save in " + modelLoadDir);
            Path modelLoadPath = new Path(modelLoadDir);
            FileSystem fs = modelLoadPath.getFileSystem(conf);
            FileStatus[] fileStatus = fs.listStatus(modelLoadPath);
            if(fileStatus == null || fileStatus.length == 0) {
              LOG.error("can not find any models in " + modelLoadDir);
              return "FAILED";
            }

            List<String> modelNameList = new ArrayList<>();
            for(int i = 0; i < fileStatus.length; i++) {
              if(fileStatus[i].isDirectory()) {
                modelNameList.add(fileStatus[i].getPath().getName());
              }
            }
            if(modelNameList.isEmpty()) {
              LOG.error("can not find any models in " + modelLoadDir);
              return "FAILED";
            }

            modelNames = modelNameList.toArray(new String[0]);
          } else {
            modelNames = needConvertModelNames.split(",");
            if(modelNames.length == 0) {
              LOG.error("can not find any models in " + modelLoadDir);
              return "FAILED";
            }
          }

          for(int i = 0; i < modelNames.length; i++) {
            LOG.info("===================start to convert model " + modelNames[i]);
            convert(conf, modelLoadDir + Path.SEPARATOR + modelNames[i],
              convertedModelSaveDir + Path.SEPARATOR + modelNames[i], serde);
            LOG.info("===================end to convert model " + modelNames[i]);
          }

          return "SUCCESS";
        }
      });
    } catch (Throwable e) {
      LOG.fatal("convert models failed.", e);
      return;
    }
  }
}
