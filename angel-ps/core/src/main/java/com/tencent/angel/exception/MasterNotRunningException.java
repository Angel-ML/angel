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

package com.tencent.angel.exception;

import java.io.IOException;

/**
 * Thrown if the master is not running
 */
public class MasterNotRunningException extends IOException {
  private static final long serialVersionUID = 1L << 23 - 1L;

  /** default constructor */
  public MasterNotRunningException() {
    super();
  }

  /**
   * Constructor
   * 
   * @param s message
   */
  public MasterNotRunningException(String s) {
    super(s);
  }

  /**
   * Constructor taking another exception.
   * 
   * @param e Exception to grab data from.
   */
  public MasterNotRunningException(Exception e) {
    super(e);
  }

  public MasterNotRunningException(String s, Exception e) {
    super(s, e);
  }
}
