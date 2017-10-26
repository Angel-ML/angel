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

package com.tencent.angel.tools;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.ModelFilesMeta;
import com.tencent.angel.utils.ConfUtils;
import com.tencent.angel.utils.Sort;
import com.tencent.angel.utils.UGITools;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Model partition merge and convert tools. It uses ${ModelLoader} to load model from model files, then
 * convert and write it to a file.
 */
public class ModelMergeAndConvert {
  private static final Log LOG = LogFactory.getLog(ModelMergeAndConvert.class);
  private final static int SPARSE_DOUBLE = 1;
  private final static int DENSE_DOUBLE = 2;
  private final static int SPARSE_INT = 3;
  private final static int DENSE_INT = 4;
  private final static int DENSE_FLOAT = 6;
  private final static int SPARSE_FLOAT = 7;
  private final static int SPARSE_DOUBLE_LONGKEY = 8;
  private final static String dataFile = "data";

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
    Path outputFile = new Path(convertedModelDir, dataFile);
    FileSystem fs = outputFile.getFileSystem(conf);
    FSDataOutputStream output = fs.create(outputFile);
    convertHeader(meta, output);

    switch (meta.getRowType()) {
      case DENSE_DOUBLE:{
        convertDenseDoubleModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case SPARSE_DOUBLE:{
        convertSparseDoubleModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case SPARSE_DOUBLE_LONGKEY :{
        convertSparseDoubleLongKeyModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case DENSE_FLOAT:{
        convertDenseFloatModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case SPARSE_FLOAT:{
        convertSparseFloatModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case DENSE_INT :{
        convertDenseIntModel(conf, output, modelInputDir, lineConvert);
        break;
      }

      case SPARSE_INT:{
        convertSparseIntModel(conf, output, modelInputDir, lineConvert);
        break;
      }
    }

    output.close();
  }

  private static void convertDenseDoubleModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    double[][] data = ModelLoader.loadToDoubleArrays(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      double[] row = data[i];
      data[i] = null;
      lineConvert.convertRowIndex(output, i);
      for(int j = 0; j < row.length; j++) {
        lineConvert.convertDouble(output, j, row[j]);
      }
    }
  }

  private static void convertSparseDoubleModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    Int2DoubleOpenHashMap [] data = ModelLoader.loadToDoubleMaps(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      Int2DoubleOpenHashMap row = data[i];
      data[i] = null;
      if(row == null) {
        continue;
      }

      lineConvert.convertRowIndex(output, i);
      int [] indexes = row.keySet().toIntArray();
      double [] values = row.values().toDoubleArray();
      row = null;
      Sort.quickSort(indexes, values, 0, indexes.length - 1);
      for(int j = 0; j < indexes.length; j++) {
        lineConvert.convertDouble(output, indexes[j], values[j]);
      }
    }
  }

  private static void convertSparseDoubleLongKeyModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    Long2DoubleOpenHashMap[] data = ModelLoader.loadToDoubleLongKeyMaps(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      Long2DoubleOpenHashMap row = data[i];
      data[i] = null;
      if(row == null) {
        continue;
      }

      lineConvert.convertRowIndex(output, i);
      long [] indexes = row.keySet().toLongArray();
      double [] values = row.values().toDoubleArray();
      row = null;
      Sort.quickSort(indexes, values, 0, indexes.length - 1);
      for(int j = 0; j < indexes.length; j++) {
        lineConvert.convertDoubleLongKey(output, indexes[j], values[j]);
      }
    }
  }

  private static void convertDenseFloatModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    float[][] data = ModelLoader.loadToFloatArrays(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      float[] row = data[i];
      data[i] = null;
      lineConvert.convertRowIndex(output, i);
      for(int j = 0; j < row.length; j++) {
        lineConvert.convertFloat(output, j, row[j]);
      }
    }
  }

  private static void convertSparseFloatModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    Int2FloatOpenHashMap[] data = ModelLoader.loadToFloatMaps(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      Int2FloatOpenHashMap row = data[i];
      data[i] = null;
      if(row == null) {
        continue;
      }

      lineConvert.convertRowIndex(output, i);
      int [] indexes = row.keySet().toIntArray();
      float [] values = row.values().toFloatArray();
      row = null;
      Sort.quickSort(indexes, values, 0, indexes.length - 1);
      for(int j = 0; j < indexes.length; j++) {
        lineConvert.convertFloat(output, indexes[j], values[j]);
      }
    }
  }

  private static void convertDenseIntModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    int[][] data = ModelLoader.loadToIntArrays(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      int[] row = data[i];
      data[i] = null;
      lineConvert.convertRowIndex(output, i);
      for(int j = 0; j < row.length; j++) {
        lineConvert.convertInt(output, j, row[j]);
      }
    }
  }

  private static void convertSparseIntModel(Configuration conf, FSDataOutputStream output,
    String modelInputDir, ModelLineConvert lineConvert) throws IOException {
    Int2IntOpenHashMap[] data = ModelLoader.loadToIntMaps(modelInputDir, conf);
    for(int i = 0; i < data.length; i++) {
      Int2IntOpenHashMap row = data[i];
      data[i] = null;
      if(row == null) {
        continue;
      }

      lineConvert.convertRowIndex(output, i);
      int [] indexes = row.keySet().toIntArray();
      int [] values = row.values().toIntArray();
      row = null;
      Sort.quickSort(indexes, values, 0, indexes.length - 1);
      for(int j = 0; j < indexes.length; j++) {
        lineConvert.convertFloat(output, indexes[j], values[j]);
      }
    }
  }

  private static void convertHeader(ModelFilesMeta meta, FSDataOutputStream output) throws IOException {
    output.writeBytes("modelName=" + meta.getMatrixName() + "\n");
    output.writeBytes("row=" + meta.getRow() + "\n");
    output.writeBytes("column=" + meta.getCol() + "\n");
    switch (meta.getRowType()) {
      case DENSE_DOUBLE:{
        output.writeBytes("rowType=T_DOUBLE_DENSE\n");
        break;
      }

      case SPARSE_DOUBLE:{
        output.writeBytes("rowType=T_DOUBLE_SPARSE\n");
        break;
      }

      case SPARSE_DOUBLE_LONGKEY :{
        output.writeBytes("rowType=T_DOUBLE_SPARSE_LONGKEY\n");
        break;
      }

      case DENSE_FLOAT:{
        output.writeBytes("rowType=T_FLOAT_DENSE\n");
        break;
      }

      case SPARSE_FLOAT:{
        output.writeBytes("rowType=T_FLOAT_SPARSE\n");
        break;
      }

      case DENSE_INT :{
        output.writeBytes("rowType=T_INT_DENSE\n");
        break;
      }

      case SPARSE_INT:{
        output.writeBytes("rowType=T_INT_SPARSE\n");
        break;
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
