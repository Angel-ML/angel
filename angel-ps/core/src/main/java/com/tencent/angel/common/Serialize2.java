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
package com.tencent.angel.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serialize2 {
  /**
   * Serialize object to the Output stream.
   *
   * @param output the Netty ByteBuf
   */
  void serialize(DataOutputStream output) throws IOException;

  /**
   * Deserialize object from the input stream.
   *
   * @param input the input stream
   */
  void deserialize(DataInputStream input) throws IOException;

  /**
   * Estimate serialized data size of the object, it used to ByteBuf allocation.
   *
   * @return int serialized data size of the object
   */
  int dataSize();
}
