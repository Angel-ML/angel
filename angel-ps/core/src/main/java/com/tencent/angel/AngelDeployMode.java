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


package com.tencent.angel;

/**
 * Angel deploy mode.
 *
 * <ul>
 *   <li>LOCAL: single JVM, multi-thread PS/Worker (mainly for tests).</li>
 *   <li>EMBEDDED: AM is embedded in the caller process (e.g. Spark Driver), used by spark-on-angel.</li>
 *   <li>YARN: launch AM/PS/Worker as YARN containers on a YARN cluster.</li>
 *   <li>STANDALONE: AM runs in the submitting JVM and PS/Worker run as independent local JVM
 *       processes started by ProcessBuilder. Does NOT depend on YARN or Spark.</li>
 *   <li>KUBERNETES: launch Master/PS/Worker pods using the Kubernetes API.</li>
 * </ul>
 */
public enum AngelDeployMode {
  LOCAL, YARN, EMBEDDED, STANDALONE, KUBERNETES
}
