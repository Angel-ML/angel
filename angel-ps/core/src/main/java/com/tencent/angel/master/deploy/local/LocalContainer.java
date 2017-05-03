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

package com.tencent.angel.master.deploy.local;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;

public class LocalContainer extends Container{

  @Override
  public int compareTo(Container o) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public ContainerId getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setId(ContainerId id) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public NodeId getNodeId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getNodeHttpAddress() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Resource getResource() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setResource(Resource resource) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Priority getPriority() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setPriority(Priority priority) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public Token getContainerToken() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setContainerToken(Token containerToken) {
    // TODO Auto-generated method stub
    
  }

}
