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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.example;


import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.utils.ModelParse;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ModelParseTask extends TrainTask<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(ModelParseTask.class);


  public ModelParseTask(TaskContext taskContext){
    super(taskContext);
  }


  @Override
  public LabeledData parse(LongWritable key, Text value) {
    return null;
  }

  @Override
  public void train(TaskContext taskContext) throws AngelException {
    Configuration conf = taskContext.getConf();
    String inputStr = conf.get(AngelConfiguration.ANGEL_TRAIN_DATA_PATH);
    String outputStr = conf.get(AngelConfiguration.ANGEL_PARSE_MODEL_PATH);
    String modelName = conf.get(AngelConfiguration.ANGEL_MODEL_PARSE_NAME);
    int convertThreadCount = conf.getInt(AngelConfiguration.ANGEL_MODEL_PARSE_THREAD_COUNT,
            AngelConfiguration.DEFAULT_ANGEL_MODEL_PARSE_THREAD_COUNT);
    ModelParse modelLoader = new ModelParse(inputStr, outputStr, modelName, convertThreadCount);

    try{
      modelLoader.convertModel();
    } catch (Exception x) {
      throw new AngelException(x);
    }
  }

}
