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

package com.tencent.angel.utils;

public class ThreadUtils {
  public static RuntimeException launderThrowable(Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException) t;
    } else if (t instanceof Error) {
      throw (Error) t;
    } else {
      throw new IllegalStateException("Not unchecked", t);
    }
  }

  /**
   * Cause the current thread to sleep as close as possible to the provided number of milliseconds.
   * This method will log and ignore any {@link InterruptedException} encountered.
   * 
   * @param millis the number of milliseconds for the current thread to sleep
   */
  public static void sleepAtLeastIgnoreInterrupts(long millis) {
    long start = Time.now();
    while (Time.now() - start < millis) {
      long timeToSleep = millis - (Time.now() - start);
      try {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException ie) {
        // LOG.warn("interrupted while sleeping", ie);
      }
    }
  }
}
