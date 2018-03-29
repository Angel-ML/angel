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

package com.tencent.angel.ml.toolkits.modelconverter;


import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.tools.ModelConverter;
import com.tencent.angel.tools.ModelLineConvert;
import com.tencent.angel.tools.TextModelLineConvert;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Use model convert tool to convert model with binary format to text format
 */
public class ModelConverterTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(ModelConverterTask.class);

  public ModelConverterTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void preProcess(TaskContext taskContext) {
    return;
  }

  @Override public void run(TaskContext taskContext) throws AngelException {
    try {
      // Get input path, output path
      String modelLoadDir = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH);
      if (modelLoadDir == null) {
        throw new InvalidParameterException(
          "convert source path " + AngelConf.ANGEL_LOAD_MODEL_PATH + " must be set");
      }

      String convertedModelSaveDir = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH);
      if (convertedModelSaveDir == null) {
        throw new InvalidParameterException(
          "converted model save path " + AngelConf.ANGEL_LOAD_MODEL_PATH + " must be set");
      }

      // Init serde
      String modelSerdeClass =
        conf.get("angel.modelconverts.serde.class", TextModelLineConvert.class.getName());
      Class<? extends ModelLineConvert> funcClass =
        (Class<? extends ModelLineConvert>) Class.forName(modelSerdeClass);
      Constructor<? extends ModelLineConvert> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      ModelLineConvert serde = constructor.newInstance();

      // Parse need convert model names, if not set, we will convert all models in input directory
      String needConvertModelNames = conf.get("angel.modelconverts.model.names");
      String[] modelNames = null;
      if (needConvertModelNames == null) {
        LOG.info("we will convert all models save in " + modelLoadDir);
        Path modelLoadPath = new Path(modelLoadDir);
        FileSystem fs = modelLoadPath.getFileSystem(conf);
        FileStatus[] fileStatus = fs.listStatus(modelLoadPath);
        if (fileStatus == null || fileStatus.length == 0) {
          throw new IOException("can not find any models in " + modelLoadDir);
        }

        List<String> modelNameList = new ArrayList<>();
        for (int i = 0; i < fileStatus.length; i++) {
          if (fileStatus[i].isDirectory()) {
            modelNameList.add(fileStatus[i].getPath().getName());
          }
        }
        if (modelNameList.isEmpty()) {
          throw new IOException("can not find any models in " + modelLoadDir);
        }

        modelNames = modelNameList.toArray(new String[0]);
      } else {
        modelNames = needConvertModelNames.split(",");
        if (modelNames.length == 0) {
          throw new IOException("can not find any models in " + modelLoadDir);
        }
      }

      for (int i = 0; i < modelNames.length; i++) {
        LOG.info("===================start to convert model " + modelNames[i]);
        ModelConverter.convert(conf, modelLoadDir + Path.SEPARATOR + modelNames[i],
          convertedModelSaveDir + Path.SEPARATOR + modelNames[i], serde);
        LOG.info("===================end to convert model " + modelNames[i]);
      }

    } catch (Throwable x) {
      LOG.fatal("convert model falied, ", x);
      throw new AngelException(x);
    }
  }
}
