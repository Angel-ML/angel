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
package com.tencent.angel.graph.utils;

import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerAnyAnyRow;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;

public class GraphMatrixUtils {

    public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
                                                   PartitionGetParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
                                                   PartitionUpdateParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
                                                   GeneralPartGetParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerLongIntRow getPSLongKeyIntRow(PSContext psContext,
                                                      PartitionGetParam partParam, int rowId) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerLongIntRow) (((RowBasedPartition) part).getRow(rowId));
    }

    public static ServerLongAnyRow getPSLongKeyRow(PSContext psContext,
                                                   GeneralPartUpdateParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
                                                 PartitionGetParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
                                                 PartitionUpdateParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
                                                 GeneralPartGetParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerIntAnyRow getPSIntKeyRow(PSContext psContext,
                                                 GeneralPartUpdateParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerIntAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerAnyAnyRow getPSAnyKeyRow(PSContext psContext,
                                                 PartitionUpdateParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerAnyAnyRow) (((RowBasedPartition) part).getRow(0));
    }

    public static ServerAnyAnyRow getPSAnyKeyRow(PSContext psContext,
                                                 PartitionGetParam partParam) {
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        return (ServerAnyAnyRow) (((RowBasedPartition) part).getRow(0));
    }
}