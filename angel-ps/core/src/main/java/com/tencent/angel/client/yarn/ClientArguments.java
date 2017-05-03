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

package com.tencent.angel.client.yarn;

public class ClientArguments {
  private String addJars = null;
  private String files = null;
  private String archives = null;
  private String userJar = null;
  private String userClass = null;
  // private userArgs: Seq[String] = Seq[String]()
  private int workerMemory = 1024; // MB
  private int workerCores = 1;
  private int numWorkers = 100;

  private int amMemory = 1024; // MB
  private int amCores = 1;
  private String amClass = "deploy.yarn.ApplicationMaster";
  private String appName = "Angel";
  private int priority = 0;
  private String queueName = null;

  public ClientArguments(String[] args) {
    this.parseArgs(args);
  }

  public String getAddJars() {
    return addJars;
  }

  public String getFiles() {
    return files;
  }

  public String getArchives() {
    return archives;
  }

  public String getUserJar() {
    return userJar;
  }

  public String getUserClass() {
    return userClass;
  }

  public int getWorkerMemory() {
    return workerMemory;
  }

  public int getWorkerCores() {
    return workerCores;
  }

  public int getAmMemory() {
    return amMemory;
  }

  public int getAmCores() {
    return amCores;
  }

  public String getAmClass() {
    return amClass;
  }

  public String getAppName() {
    return appName;
  }

  public int getPriority() {
    return priority;
  }

  public String getQueueName() {
    return queueName;
  }

  public int getWorkerNum() {
    return numWorkers;
  }

  private void parseArgs(String[] inputArgs) {
    // val userArgsBuffer = new ArrayBuffer[String]()
    // List<String> args = inputArgs;
    int len = inputArgs.length;
    String arg = null;
    // String errMsg = "";
    if (len == 0) {
      throw new IllegalArgumentException(getUsageMessage());
    }

    for (int i = 0; i < len; i++) {
      arg = inputArgs[i];
      if (i + 1 >= len) {
        break;
      }
      if (arg.equals("--jar")) {
        userJar = inputArgs[i + 1];
      } else if (arg.equals("--class")) {
        userClass = inputArgs[i + 1];
      } else if (arg.equals("--master-class") || arg.equals("--am-class")) {
        amClass = inputArgs[i + 1];
      } else if (arg.equals("--master-memory") || arg.equals("--am-memory")) {
        try {
          amMemory = Integer.valueOf(inputArgs[i + 1]);
        } catch (Exception x) {
          throw new IllegalArgumentException(
              "unvalid value for \"--master-memory\" or \"--am-memory\"" + " value:"
                  + inputArgs[i + 1]);
        }
      } else if (arg.equals("--master-cores") || arg.equals("--am-cores")) {
        try {
          amCores = Integer.valueOf(inputArgs[i + 1]);
        } catch (Exception x) {
          throw new IllegalArgumentException(
              "unvalid value for \"--master-cores\" or \"--am-cores\"" + " value:"
                  + inputArgs[i + 1]);
        }
      } else if (arg.equals("--num-workers")) {
        try {
          numWorkers = Integer.valueOf(inputArgs[i + 1]);
        } catch (Exception x) {
          throw new IllegalArgumentException("unvalid value for \"--num-workers\"" + " value:"
              + inputArgs[i + 1]);
        }
      } else if (arg.equals("--worker-memory")) {
        try {
          workerMemory = Integer.valueOf(inputArgs[i + 1]);
        } catch (Exception x) {
          throw new IllegalArgumentException("unvalid value for \"--worker-memory\"" + " value:"
              + inputArgs[i + 1]);
        }

      } else if (arg.equals("--worker-cores")) {
        try {
          workerCores = Integer.valueOf(inputArgs[i + 1]);
        } catch (Exception x) {
          throw new IllegalArgumentException("unvalid value for \"--worker-cores\"" + " value:"
              + inputArgs[i + 1]);
        }
      }

      else if (arg.equals("--queue")) {
        queueName = inputArgs[i + 1];
      }

      else if (arg.equals("--name")) {
        appName = inputArgs[i + 1];
      }

      else if (arg.equals("--addJars")) {
        addJars = inputArgs[i + 1];
      }

      else if (arg.equals("--files")) {
        files = inputArgs[i + 1];
      }

      else if (arg.equals("--archives")) {
        archives = inputArgs[i + 1];
      } else {
        throw new IllegalArgumentException("unsport args:" + getUsageMessage());
      }

      i += 1;
    }
  }

  private String getUsageMessage() {
    String msg =
        "Usage: org.apache.spark.deploy.yarn.Client [options] \n"
            + "Options:\n"
            + "  --jar JAR_PATH             Path to your application's JAR file (required in yarn-cluster mode)\n"
            + "  --class CLASS_NAME         Name of your application's main class (required)\n"
            + "  --arg ARG                  Argument to be passed to your application's main class.\n"
            + "                             Multiple invocations are possible, each will be passed in order.\n"
            + "  --num-workers NUM        Number of executors to start (Default: 2)\n"
            + "  --worker-cores NUM       Number of cores for the worker (Default: 1).\n"
            + "  --workers-memory MEM      Memory per executor (e.g. 1000M, 2G) (Default: 1G)\n"
            + "  --am-cores NUM       Number of cores for the appmaster (Default: 1).\n"
            + "  --am-memory MEM        Memory for driver (e.g. 1000M, 2G) (Default: 512 Mb)\n"
            + "  --name NAME                The name of your application (Default: Spark)\n"
            + "  --queue QUEUE              The hadoop queue to use for allocation requests (Default: 'default')\n"
            + "  --addJars jars             Comma separated list of local jars that want SparkContext.addJar to work with.\n"
            + "  --files files              Comma separated list of files to be distributed with the job.\n"
            + "  --archives archives        Comma separated list of archives to be distributed with the job.";
    return msg;
  }
}
