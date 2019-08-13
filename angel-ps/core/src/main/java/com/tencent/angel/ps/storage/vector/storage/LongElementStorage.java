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

import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.op.ILongElementOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Base class for long key complex value storage
 */
public abstract class LongElementStorage extends ObjectTypeStorage implements ILongElementOp {

  public LongElementStorage(Class<? extends IElement> objectClass, long indexOffset) {
    super(objectClass, indexOffset);
  }

  public LongElementStorage() {
    super(null, 0L);
  }

  abstract public ObjectIterator<Long2ObjectMap.Entry<IElement>> iterator();
}
