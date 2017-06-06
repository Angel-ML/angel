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

public class UnsupportedException extends RuntimeException {

  private static final long serialVersionUID = 3317332996281996367L;

  public UnsupportedException() {
    super();
  }

  public UnsupportedException(String arg0, Throwable arg1, boolean arg2, boolean arg3) {
    super(arg0, arg1, arg2, arg3);
  }

  public UnsupportedException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public UnsupportedException(String arg0) {
    super(arg0);
  }

  public UnsupportedException(Throwable arg0) {
    super(arg0);
  }
}
