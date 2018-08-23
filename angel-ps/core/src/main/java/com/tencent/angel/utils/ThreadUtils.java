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


package com.tencent.angel.utils;

import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;

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

  public static String toString(ThreadInfo tInfo) {
    StringBuilder sb = new StringBuilder(
      "\"" + tInfo.getThreadName() + "\"" + " Id=" + tInfo.getThreadId() + " " + tInfo
        .getThreadState());
    if (tInfo.getLockName() != null) {
      sb.append(" on " + tInfo.getLockName());
    }
    if (tInfo.getLockOwnerName() != null) {
      sb.append(" owned by \"" + tInfo.getLockOwnerName() + "\" Id=" + tInfo.getLockOwnerId());
    }
    if (tInfo.isSuspended()) {
      sb.append(" (suspended)");
    }
    if (tInfo.isInNative()) {
      sb.append(" (in native)");
    }
    sb.append('\n');
    int i = 0;

    StackTraceElement[] stackTrace = tInfo.getStackTrace();
    for (; i < stackTrace.length; i++) {
      StackTraceElement ste = stackTrace[i];
      sb.append("\tat " + ste.toString());
      sb.append('\n');
      if (i == 0 && tInfo.getLockInfo() != null) {
        Thread.State ts = tInfo.getThreadState();
        switch (ts) {
          case BLOCKED:
            sb.append("\t-  blocked on " + tInfo.getLockInfo());
            sb.append('\n');
            break;
          case WAITING:
            sb.append("\t-  waiting on " + tInfo.getLockInfo());
            sb.append('\n');
            break;
          case TIMED_WAITING:
            sb.append("\t-  waiting on " + tInfo.getLockInfo());
            sb.append('\n');
            break;
          default:
        }
      }

      MonitorInfo[] lockedMonitors = tInfo.getLockedMonitors();
      for (MonitorInfo mi : lockedMonitors) {
        if (mi.getLockedStackDepth() == i) {
          sb.append("\t-  locked " + mi);
          sb.append('\n');
        }
      }
    }

    LockInfo[] locks = tInfo.getLockedSynchronizers();
    if (locks.length > 0) {
      sb.append("\n\tNumber of locked synchronizers = " + locks.length);
      sb.append('\n');
      for (LockInfo li : locks) {
        sb.append("\t- " + li);
        sb.append('\n');
      }
    }
    sb.append('\n');
    return sb.toString();
  }
}