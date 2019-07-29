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

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

/**
 * Base class for complex type storage
 */
public abstract class ObjectTypeStorage extends Storage {

  /**
   * Object class
   */
  protected Class<? extends IElement> objectClass;

  public ObjectTypeStorage(Class<? extends IElement> objectClass, long indexOffset) {
    super(indexOffset);
    this.objectClass = objectClass;
  }

  protected IElement newElement() {
    try {
      return objectClass.newInstance();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    throw new UnsupportedOperationException("pipeline add/set is not support for complex type now");
  }
}
