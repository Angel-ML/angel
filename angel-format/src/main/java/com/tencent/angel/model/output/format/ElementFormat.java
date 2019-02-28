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

package com.tencent.angel.model.output.format;

import com.tencent.angel.model.output.element.IntDoubleElement;
import com.tencent.angel.model.output.element.IntFloatElement;
import com.tencent.angel.model.output.element.IntIntElement;
import com.tencent.angel.model.output.element.IntLongElement;
import com.tencent.angel.model.output.element.LongDoubleElement;
import com.tencent.angel.model.output.element.LongFloatElement;
import com.tencent.angel.model.output.element.LongIntElement;
import com.tencent.angel.model.output.element.LongLongElement;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Element format interface
 */
public interface ElementFormat extends Format {

  /**
   * Save a (int, float) element
   *
   * @param element a (int, float) element
   * @param out output stream
   */
  void save(IntFloatElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, double) element
   *
   * @param element a (int, double) element
   * @param out output stream
   */
  void save(IntDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, int) element
   *
   * @param element a (int, int) element
   * @param out output stream
   */
  void save(IntIntElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, long) element
   *
   * @param element a (int, long) element
   * @param out output stream
   */
  void save(IntLongElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, float) element
   *
   * @param element a (long, float) element
   * @param out output stream
   */
  void save(LongFloatElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, double) element
   *
   * @param element a (long, double) element
   * @param out output stream
   */
  void save(LongDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, int) element
   *
   * @param element a (long, int) element
   * @param out output stream
   */
  void save(LongIntElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, long) element
   *
   * @param element a (long, long) element
   * @param out output stream
   */
  void save(LongLongElement element, DataOutputStream out) throws IOException;

  /**
   * Load a (int, float) element
   *
   * @param element a (int, float) element
   * @param in input stream
   */
  void load(IntFloatElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, double) element
   *
   * @param element a (int, double) element
   * @param in input stream
   */
  void load(IntDoubleElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, int) element
   *
   * @param element a (int, int) element
   * @param in input stream
   */
  void load(IntIntElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, long) element
   *
   * @param element a (int, long) element
   * @param in input stream
   */
  void load(IntLongElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, float) element
   *
   * @param element a (long, float) element
   * @param in input stream
   */
  void load(LongFloatElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, double) element
   *
   * @param element a (long, double) element
   * @param in input stream
   */
  void load(LongDoubleElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, int) element
   *
   * @param element a (long, int) element
   * @param in input stream
   */
  void load(LongIntElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, long) element
   *
   * @param element a (long, long) element
   * @param in input stream
   */
  void load(LongLongElement element, DataInputStream in) throws IOException;

}
