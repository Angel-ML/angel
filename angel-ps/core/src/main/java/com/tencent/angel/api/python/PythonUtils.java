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


package com.tencent.angel.api.python;

import com.tencent.angel.conf.AngelConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import scala.Int;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class PythonUtils {
  private static final Log LOG = LogFactory.getLog(PythonRunner.class);

  /**
   * Get the python PYTHONPATH for pyAngel, either from ANGEL_HOME, or added jar.
   *
   * @return
   */
  public static String getAngelPythonPath() {
    ArrayList<String> pythonPath = new ArrayList<>();
    String angelHome = System.getenv("ANGEL_HOME");
    pythonPath.add(String.join(File.separator, angelHome, "lib", "py4j-0.10.4-src.zip"));
    pythonPath.add(String.join(File.separator, angelHome, "lib", "pyangel.zip"));
    pythonPath.add(String.join(File.separator, angelHome, "python", ""));
    pythonPath.add(AngelConf.create().get(AngelConf.ANGEL_JOB_LIBJARS));

    return String.join(File.pathSeparator, pythonPath);
  }

  /**
   * Convert a java.util.Map of properties to a org.apache.hadoop.conf.Configuration
   */
  public static Configuration converseMapToConf(Map<String, Object> map) {
    Configuration conf = new Configuration();
    return addMapToConf(map, conf);
  }

  public static Configuration addMapToConf(Map<String, Object> map, Configuration conf) {
    for (String key : map.keySet()) {
      // To-DO: add other ways to justify different value types
      // This is so ugly, must re-implement by more elegance way
      if (map.get(key) instanceof String) {
        conf.set(key, (String) map.get(key));
      } else if (map.get(key) instanceof Integer) {
        conf.setInt(key, (Integer) map.get(key));
      } else if (map.get(key) instanceof Boolean) {
        conf.setBoolean(key, (Boolean) map.get(key));
      } else if (map.get(key) instanceof Float) {
        conf.setFloat(key, (Float) map.get(key));
      } else if (map.get(key) instanceof Double) {
        conf.setDouble(key, (Double) map.get(key));
      } else if (map.get(key) instanceof Long) {
        conf.setLong(key, (Long) map.get(key));
      } else {
        LOG.error("Parse value failed");
      }
    }
    return conf;
  }
}
