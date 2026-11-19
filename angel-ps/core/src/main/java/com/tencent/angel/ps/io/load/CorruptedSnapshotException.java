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

package com.tencent.angel.ps.io.load;

import java.io.IOException;

/**
 * Marker subclass of {@link IOException} thrown by {@link SnapshotRecover#getSnapshotPath}
 * when it detects that the on-HDFS snapshot layout is structurally present
 * (one or more integer-named epoch sub-directories exist) but every single one
 * is empty — i.e. the previous PS process was killed between
 * {@link com.tencent.angel.ps.io.save.SnapshotDumper}'s {@code mkdir tmp} and
 * the atomic rename, leaving a "phantom" snapshot.
 *
 * <p>This is distinct from a generic transient HDFS error (e.g. NameNode rejection,
 * permission failure) because:
 * <ul>
 *   <li>retrying does not help — the corrupted state will not magically heal;</li>
 *   <li>silently falling through to "no snapshot → use InitFunc / empty matrix"
 *       would zero out any algorithm that depends on previously saved data
 *       (embeddings, GNN weights, trained models) without the user being told.</li>
 * </ul>
 *
 * <p>Caller contract: {@link com.tencent.angel.ps.ParameterServer#initMatricesData}
 * MUST NOT swallow this exception the way it does for generic {@link IOException}.
 * Re-throwing it kills the current PS attempt, lets the user see the diagnostic,
 * and forces a deliberate cleanup step before re-running — instead of letting the
 * job march on with a silently empty matrix.
 */
public class CorruptedSnapshotException extends IOException {
  public CorruptedSnapshotException(String message) {
    super(message);
  }
}
