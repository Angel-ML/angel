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
package com.tencent.angel.ipc;

import com.tencent.angel.conf.TConstants;

/**
 * Utility used by client connections such as {@link ServerCallable}
 */
public class ConnectionUtils {
  /**
   * Calculate pause time.
   * 
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= TConstants.RETRY_BACKOFF.length) {
      ntries = TConstants.RETRY_BACKOFF.length - 1;
    }
    return pause * TConstants.RETRY_BACKOFF[ntries];
  }
}
