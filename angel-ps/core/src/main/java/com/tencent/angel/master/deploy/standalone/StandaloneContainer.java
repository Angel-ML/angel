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

package com.tencent.angel.master.deploy.standalone;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;

/**
 * Container placeholder used by {@link com.tencent.angel.AngelDeployMode#STANDALONE} mode.
 * The actual lifecycle of the underlying process is tracked by
 * {@link StandaloneContainerLauncher}.
 */
public class StandaloneContainer extends Container {

  @Override public int compareTo(Container o) {
    return 0;
  }

  @Override public ContainerId getId() {
    return null;
  }

  @Override public void setId(ContainerId id) {
  }

  @Override public NodeId getNodeId() {
    return null;
  }

  @Override public void setNodeId(NodeId nodeId) {
  }

  @Override public String getNodeHttpAddress() {
    return null;
  }

  @Override public void setNodeHttpAddress(String nodeHttpAddress) {
  }

  @Override public Resource getResource() {
    return null;
  }

  @Override public void setResource(Resource resource) {
  }

  @Override public Priority getPriority() {
    return null;
  }

  @Override public void setPriority(Priority priority) {
  }

  @Override public Token getContainerToken() {
    return null;
  }

  @Override public void setContainerToken(Token containerToken) {
  }
}
