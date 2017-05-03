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

package com.tencent.angel.client;

import java.util.Map;
import java.util.Map.Entry;

public class PSStartUpConfig {
  private int psNum;
  private int psMemoryInMB;
  private int psVcoreNum;
  private int psAgentNum;
  private int psAgentMemoryInMB;
  private int psAgentVcoreNum;
  private Map<String, String> config;

  public PSStartUpConfig(int psNum, int psMemoryInMB, int psVcoreNum, int psAgentNum,
      int psAgentMemoryInMB, int psAgentVcoreNum, Map<String, String> config) {
    this.psNum = psNum;
    this.psMemoryInMB = psMemoryInMB;
    this.psVcoreNum = psVcoreNum;
    this.psAgentNum = psAgentNum;
    this.psAgentMemoryInMB = psAgentMemoryInMB;
    this.psAgentVcoreNum = psAgentVcoreNum;
    this.config = config;
  }

  public int getPsNum() {
    return psNum;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public int getPsMemoryInMB() {
    return psMemoryInMB;
  }

  public int getPsVcoreNum() {
    return psVcoreNum;
  }

  public int getPsAgentNum() {
    return psAgentNum;
  }

  public int getPsAgentMemoryInMB() {
    return psAgentMemoryInMB;
  }

  public int getPsAgentVcoreNum() {
    return psAgentVcoreNum;
  }

  public void setPsNum(int psNum) {
    this.psNum = psNum;
  }

  public void setPsMemoryInMB(int psMemoryInMB) {
    this.psMemoryInMB = psMemoryInMB;
  }

  public void setPsVcoreNum(int psVcoreNum) {
    this.psVcoreNum = psVcoreNum;
  }

  public void setPsAgentNum(int psAgentNum) {
    this.psAgentNum = psAgentNum;
  }

  public void setPsAgentMemoryInMB(int psAgentMemoryInMB) {
    this.psAgentMemoryInMB = psAgentMemoryInMB;
  }

  public void setPsAgentVcoreNum(int psAgentVcoreNum) {
    this.psAgentVcoreNum = psAgentVcoreNum;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public String toString() {
    return "PSStartUpConfig [psNum=" + psNum + ", psMemoryInMB=" + psMemoryInMB + ", psVcoreNum="
        + psVcoreNum + ", psAgentNum=" + psAgentNum + ", psAgentMemoryInMB=" + psAgentMemoryInMB
        + ", psAgentVcoreNum=" + psAgentVcoreNum + ", config=" + mapToString(config) + "]";
  }

  private String mapToString(Map<String, String> config) {
    StringBuilder sb = new StringBuilder();
    for (Entry<String, String> entry : config.entrySet()) {
      sb.append(entry.getKey());
      sb.append(":");
      sb.append(entry.getValue());
      sb.append("\n");
    }

    return sb.toString();
  }
}
