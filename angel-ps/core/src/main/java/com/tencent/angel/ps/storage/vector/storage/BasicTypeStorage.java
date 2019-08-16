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

package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.BasicTypePipelineOp;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Base class for basic type storage
 */
public abstract class BasicTypeStorage extends Storage implements BasicTypePipelineOp {

  public BasicTypeStorage(long indexOffset) {
    super(indexOffset);
  }

  public BasicTypeStorage() {
    this(0L);
  }

  public void serialize(DataOutputStream output) throws IOException {
    throw new UnsupportedOperationException("Unsupport now");
  }

  public void deserialize(DataInputStream input) throws IOException {
    throw new UnsupportedOperationException("Unsupport now");
  }

  public int dataLen() {
    return bufferLen();
  }
}
