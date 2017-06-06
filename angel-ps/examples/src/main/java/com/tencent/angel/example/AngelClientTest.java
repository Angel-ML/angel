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

import com.tencent.angel.client.AngelPSClient;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.exception.InvalidParameterException;

import com.tencent.angel.ml.matrix.MatrixContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Java class running tools for Angel.
 */
public class AngelClientTest {
  private static final Log LOG = LogFactory.getLog(AngelClientTest.class);
  private static final String angelSysConfFile = "angel-site.xml";

  public static void main(String[] args) throws Exception {
    try {
      final Configuration conf = new Configuration();

      // load angel system configuration
      String angelHomePath = System.getenv("ANGEL_HOME");
      if (angelHomePath == null) {
        LOG.fatal("ANGEL_HOME is empty, please set it first");
        throw new InvalidParameterException("ANGEL_HOME is empty, please set it first");
      }
      LOG.info("angelHomePath conf path=" + angelHomePath + "/conf/" + angelSysConfFile);
      conf.addResource(new Path(angelHomePath + "/conf/" + angelSysConfFile));
      LOG.info("load system config file success");

      String hadoopHomePath = System.getenv("HADOOP_HOME");
      if (hadoopHomePath == null) {
        LOG.warn("HADOOP_HOME is empty.");
      } else {
        conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/yarn-site.xml"));
        conf.addResource(new Path(hadoopHomePath + "/etc/hadoop/hdfs-site.xml"));
      }

      // load user configuration:
      // 1. user config file
      // 2. command lines
      Map<String, String> cmdConfMap = parseArgs(args);
      if (cmdConfMap.containsKey(AngelConfiguration.ANGEL_APP_CONFIG_FILE)) {
        LOG.info("user app config file " + cmdConfMap.get(AngelConfiguration.ANGEL_APP_CONFIG_FILE));
        conf.addResource(new Path(cmdConfMap.get(AngelConfiguration.ANGEL_APP_CONFIG_FILE)));
      }

      // add user resource files to "angel.lib.jars" to upload to hdfs
      if (cmdConfMap.containsKey(AngelConfiguration.ANGEL_APP_USER_RESOURCE_FILES)) {
        addResourceFiles(conf, cmdConfMap.get(AngelConfiguration.ANGEL_APP_USER_RESOURCE_FILES));
      }

      for (Entry<String, String> kvEntry : cmdConfMap.entrySet()) {
        conf.set(kvEntry.getKey(), kvEntry.getValue());
      }

      // load user job jar if it exist
      String jobJar = conf.get(AngelConfiguration.ANGEL_JOB_JAR);
      if (jobJar != null) {
        loadJar(jobJar);
        addResourceFiles(conf, jobJar);
      }

      // Expand the environment variable
      try {
        expandEnv(conf);
      } catch (Exception x) {
        LOG.warn("expand env in configuration failed.", x);
      }

      AngelPSClient psClient = new AngelPSClient(conf);
      MatrixContext w1 = new MatrixContext("w1", 1, 1000000, 1, 10000);
      MatrixContext w2 = new MatrixContext("w2", 1, 1000000, 1, 100000);
      w1.set(MatrixConfiguration.MATRIX_LOAD_PATH, "hdfs://tl-nn-tdw:54310/test/paynie/angel_clienttest1");
      w2.set(MatrixConfiguration.MATRIX_LOAD_PATH, "hdfs://tl-nn-tdw:54310/test/paynie/angel_clienttest1");

      psClient.addMatrix(w1);
      psClient.addMatrix(w2);

      psClient.startPS();
      Thread.sleep(10000);
      List<String> needSaveMatrices = new ArrayList<String>();
      needSaveMatrices.add("w1");
      needSaveMatrices.add("w2");

      psClient.save(needSaveMatrices);
      psClient.stopPS();
    } catch (Exception xx) {
      LOG.error("start angel client failed");
    }
  }

  private static void expandEnv(Configuration conf) {
    Map<String, String> kvs = conf.getValByRegex("angel.*");
    Pattern pattern = Pattern.compile("\\$\\{[\\p{Alnum}\\p{Punct}]+?\\}");

    for (Entry<String, String> kv : kvs.entrySet()) {
      String value = kv.getValue();
      Matcher matcher = pattern.matcher(value);
      List<String> keys = new ArrayList<String>();

      while (matcher.find()) {
        String matchedStr = matcher.group();
        keys.add(matchedStr.substring(2, matchedStr.length() - 1));
      }

      int size = keys.size();
      for (int i = 0; i < size; i++) {
        String envValue = System.getenv(keys.get(i));
        if (envValue == null) {
          LOG.warn("env " + keys.get(i) + " is null, please check.");
          continue;
        }
        value = value.replaceAll("\\$\\{" + keys.get(i) + "\\}", envValue);
      }

      conf.set(kv.getKey(), value);
    }

    // Add default fs(local fs) for lib jars.
    String libJars = conf.get(AngelConfiguration.ANGEL_JOB_LIBJARS);
    if (libJars != null) {
      StringBuilder sb = new StringBuilder();
      String[] jars = libJars.split(",");
      for (int i = 0; i < jars.length; i++) {
        if (new Path(jars[i]).isAbsoluteAndSchemeAuthorityNull()) {
          sb.append("file://").append(jars[i]);
          if (i != jars.length - 1) {
            sb.append(",");
          }
        } else {
          sb.append(jars[i]);
          if (i != jars.length - 1) {
            sb.append(",");
          }
        }
      }
      conf.set(AngelConfiguration.ANGEL_JOB_LIBJARS, sb.toString());
    }
  }

  private static void addResourceFiles(Configuration conf, String fileNames)
    throws MalformedURLException {
    String[] fileNameArray = fileNames.split(",");
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fileNameArray.length; i++) {
      if (i != 0) {
        sb.append(",");
      }
      URL url = new File(fileNameArray[i]).toURI().toURL();
      sb.append(url.toString());
    }

    String addJars = conf.get(AngelConfiguration.ANGEL_JOB_LIBJARS);

    if (addJars == null || addJars.trim().isEmpty()) {
      conf.set(AngelConfiguration.ANGEL_JOB_LIBJARS, sb.toString());
    } else {
      conf.set(AngelConfiguration.ANGEL_JOB_LIBJARS, sb.toString() + "," + addJars);
    }
  }

  private static Map<String, String> parseArgs(String[] args) throws InvalidParameterException {
    Map<String, String> kvMap = new HashMap<String, String>();

    int seg = 0;
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith("-D")) {
        seg = args[i].indexOf("=");
        if(seg > 0) {
          kvMap.put(args[i].substring(2, seg), args[i].substring(seg + 1));
        } else {
          throw new InvalidParameterException("unvalid parameter " + args[i]);
        }
      } else if ((seg = args[i].indexOf(":")) > 0) {
        kvMap.put(args[i].substring(0, seg), args[i].substring(seg + 1));
      } else if (args[i].endsWith(".jar")) {

      } else {
        switch (args[i]) {
          case "jar": {
            if (i == args.length - 1) {
              throw new InvalidParameterException("there must be a jar file after jar commond");
            } else {
              i++;
              kvMap.put(AngelConfiguration.ANGEL_JOB_JAR, args[i]);
            }
            break;
          }
          default: {
            throw new InvalidParameterException("unvalid parameter " + args[i]);
          }
        }
      }
    }
    return kvMap;
  }

  @SuppressWarnings("unused")
  private static boolean isValidJar(String string) {
    // TODO Auto-generated method stub
    return false;
  }

  private static void loadJar(String jarFile) throws IOException {
    URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    Class<? extends URLClassLoader> sysclass = URLClassLoader.class;
    try {
      Method method = sysclass.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);

      method.invoke(sysloader, new File(jarFile).toURI().toURL());

    } catch (Throwable t) {
      throw new IOException("Error, could not add URL to system classloader", t);
    }
  }
}
