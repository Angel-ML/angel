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

package com.tencent.angel.ps.storage.vector.op;

import com.tencent.angel.ps.storage.vector.element.IElement;

/**
 * The operator for the storage that store <element, element> values
 */
public interface IElementElementOp extends IElementKeyOp, IElementValueOp {

    /**
     * Get element value use element key
     *
     * @param key element key
     * @return element value
     */
    IElement get(IElement key);

    /**
     * Set element value
     *
     * @param key element key
     * @param value new element value
     */
    void set(IElement key, IElement value);

    /**
     * Get element values use indices
     *
     * @param keys element keys
     * @return element value
     */
    IElement[] get(IElement[] keys);

    /**
     * Set element values
     *
     * @param keys element keys
     * @param values new element values
     */
    void set(IElement[] keys, IElement[] values);
}