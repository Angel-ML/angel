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

package com.tencent.angel.master.app;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class AppEvent extends AbstractEvent<AppEventType> {

  private ApplicationId appid;

  public AppEvent(AppEventType type) {
    super(type);
  }

  public AppEvent(ApplicationId applicationID, AppEventType type) {
    super(type);
    appid = applicationID;
  }

  public ApplicationId getAppid() {
    return appid;
  }

  public void setAppid(ApplicationId appid) {
    this.appid = appid;
  }

}
