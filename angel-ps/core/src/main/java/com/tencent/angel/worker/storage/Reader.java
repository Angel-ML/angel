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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.worker.storage;

import java.io.IOException;

/**
 * The interface Reader.
 *
 * @param <K> the KEY type
 * @param <V> the VALUE type
 */
public interface Reader<K, V> {

  /**
   * Read the next key, value pair
   * 
   * @return true if a key/value pair was read
   * @throws IOException
   * @throws InterruptedException the interrupted exception
   */
  boolean nextKeyValue() throws IOException, InterruptedException;

  /**
   * Gets current key.
   *
   * @return the current key
   * @throws IOException
   * @throws InterruptedException the interrupted exception
   */
  K getCurrentKey() throws IOException, InterruptedException;

  /**
   * Gets current value.
   *
   * @return the current value
   * @throws IOException
   * @throws InterruptedException the interrupted exception
   */
  V getCurrentValue() throws IOException, InterruptedException;

  /**
   * Reset the Reader position to beginning
   *
   * @throws IOException
   */
  void reset() throws IOException;

  /**
   * Close.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Gets progress.
   *
   * @return the progress
   */
  float getProgress();
}
