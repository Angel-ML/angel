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

package com.tencent.angel.exception;

public class PSRPCException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public PSRPCException() {
  }

  public PSRPCException(String message) {
    super(message);
  }

  public PSRPCException(String message, Throwable cause) {
    super(message, cause);
  }

  public PSRPCException(Throwable cause) {
    super(cause);
  }
}
