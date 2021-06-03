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

package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.op.IElementElementOp;
import com.tencent.angel.ps.storage.vector.storage.ElementElementStorage;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public class ServerAnyAnyRow extends ServerComplexTypeRow implements IElementElementOp {

    /**
     * Create a new ServerAnyAnyRow
     *
     * @param valueType element value type class
     * @param rowId row index
     * @param rowType row type
     * @param startCol start position
     * @param endCol end position
     * @param estElemNum the estimate element number
     * @param storage vector storage
     */
    public ServerAnyAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
                              int startCol, int endCol, int estElemNum,
                              ElementElementStorage storage, RouterType routerType) {
        super(valueType, rowId, rowType, startCol, endCol, estElemNum, storage, routerType);
    }

    /**
     * Create a new ServerAnyAnyRow
     *
     * @param valueType element value type class
     * @param rowId row index
     * @param rowType row type
     * @param startCol start position
     * @param endCol end position
     * @param estElemNum the estimate element number
     */
    public ServerAnyAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
                              int startCol, int endCol, int estElemNum, RouterType routerType) {
        this(valueType, rowId, rowType, startCol, endCol, estElemNum, null, routerType);
    }

    /**
     * Create a new empty ServerAnyAnyRow
     */
    public ServerAnyAnyRow(RowType rowType) {
        this(null, 0, rowType, 0, 0, 0, null);
    }

    @Override
    public ElementElementStorage getStorage() {
        return (ElementElementStorage) storage;
    }

    @Override
    public int size() {
        return getStorage().size();
    }

    @Override
    public ServerAnyAnyRow adaptiveClone() {
        return new ServerAnyAnyRow(valueType, rowId, rowType, (int) startCol, (int) endCol,
                (int) estElemNum,
                (ElementElementStorage) getStorage().adaptiveClone(), routerType);
    }

    @Override
    public ServerAnyAnyRow deepClone() {
        return new ServerAnyAnyRow(valueType, rowId, rowType, (int) startCol, (int) endCol,
                (int) estElemNum,
                (ElementElementStorage) getStorage().deepClone(), routerType);
    }

    @Override
    public IElement get(IElement key) {
        return getStorage().get(key);
    }

    @Override
    public void set(IElement key, IElement value) {
        getStorage().set(key, value);
    }

    @Override
    public IElement[] get(IElement[] keys) {
        return getStorage().get(keys);
    }

    @Override
    public void set(IElement[] keys, IElement[] values) {
        getStorage().set(keys, values);
    }

    @Override
    public boolean exist(IElement key) {
        return getStorage().exist(key);
    }
}