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

package com.tencent.angel.master.deploy.yarn;

import com.tencent.angel.common.Id;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;


class ContainerRequest {
  final Id id;
  final Resource capability;
  final String[] hosts;
  final String[] racks;
  final Priority priority;


  public ContainerRequest(Id id, Resource capability, String[] hosts, String[] racks,
      Priority priority) {
    this.id = id;
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
    this.priority = priority;
  }

  public ContainerRequest(Id id, Resource capability, Priority priority) {
    this(id, capability, null, null, priority);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ID[").append(id).append("]");
    sb.append("Capability[").append(capability).append("]");
    sb.append("Priority[").append(priority).append("]");
    return sb.toString();
  }
}
