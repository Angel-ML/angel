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

package com.tencent.angel.common;

/**
 * Angel counters.
 *
 */
public class AngelCounter {
  public static final String GC_TIME_MILLIS = "GC_TIME_MILLIS";
  public static final String CPU_MILLISECONDS = "CPU_MILLISECONDS";
  public static final String PHYSICAL_MEMORY_BYTES = "PHYSICAL_MEMORY_BYTES";
  public static final String VIRTUAL_MEMORY_BYTES = "VIRTUAL_MEMORY_BYTES";
  public static final String COMMITTED_HEAP_BYTES = "COMMITTED_HEAP_BYTES";

  public static final String BYTES_READ = "BYTES_READ";
  public static final String BYTES_WRITTEN = "BYTES_WRITTEN";
  public static final String READ_OPS = "READ_OPS";
  public static final String LARGE_READ_OPS = "LARGE_READ_OPS";
  public static final String WRITE_OPS = "WRITE_OPS";
}
