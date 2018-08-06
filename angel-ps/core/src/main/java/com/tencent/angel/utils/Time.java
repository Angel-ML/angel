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


/**
 * Utility methods for getting the time and computing intervals.
 */
public final class Time {

  /**
   * Current system time. Do not use this to calculate a duration or interval to sleep, because it
   * will be broken by settimeofday. Instead, use monotonicNow.
   * 
   * @return current time in msec.
   */
  public static long now() {
    return System.currentTimeMillis();
  }

  /**
   * Current time from some arbitrary time base in the past, counting in milliseconds, and not
   * affected by settimeofday or similar system clock changes. This is appropriate to use when
   * computing how much longer to wait for an interval to expire.
   * 
   * @return a monotonic clock that counts in milliseconds.
   */
  public static long monotonicNow() {
    final long NANOSECONDS_PER_MILLISECOND = 1000000;

    return System.nanoTime() / NANOSECONDS_PER_MILLISECOND;
  }
}
