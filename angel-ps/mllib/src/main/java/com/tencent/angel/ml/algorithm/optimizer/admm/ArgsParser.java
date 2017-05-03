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

package com.tencent.angel.ml.algorithm.optimizer.admm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ArgsParser {

  private static final Log LOG = LogFactory.getLog(ArgsParser.class);
  public static final String KEY_VALUE_SEP = ":";

  public static Configuration parse(String[] args) {
    Configuration cmdArgs = new Configuration();
    for (int i = 0; i < args.length; i++) {
      String argv = args[i];
      int sepIdx = argv.indexOf(KEY_VALUE_SEP);
      if (sepIdx != -1) {
        String k = argv.substring(0, sepIdx).trim();
        String v = argv.substring(sepIdx + 1).trim();
        if (v != "" && v != "Nan" && v != null) {
          cmdArgs.set(k, v);
          LOG.info("Argument: " + k + " -> " + v);
        }
      }
    }
    return cmdArgs;
  }
}
