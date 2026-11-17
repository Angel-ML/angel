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

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.utils.Sort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Snapshot recover
 */
public class SnapshotRecover {
  private static final Log LOG = LogFactory.getLog(SnapshotRecover.class);
  /**
   * Snapshot base directory
   */
  private final Path baseDirPath;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create SnapshotRecover
   *
   * @param context PS context
   */
  public SnapshotRecover(PSContext context) {
    this.context = context;
    String outputDir = context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    baseDirPath = new Path(
      outputDir + Path.SEPARATOR + ModelFilesConstent.snapshotDirName + Path.SEPARATOR + context
        .getPSAttemptId().getPsId());
  }

  /**
   * Get the last written snapshot directory
   *
   * @return the last written snapshot directory
   * @throws IOException
   */
  public Path getSnapshotBasePath() throws IOException {
    return baseDirPath;
  }

  /**
   * Get the last written snapshot directory of the matrix.
   *
   * <p><b>Contract.</b> Three possible outcomes:
   * <ul>
   *   <li><b>Returns a non-null Path</b>: the path points at a snapshot epoch directory
   *       that is <em>load-ready</em>, i.e. carries either the shared meta file
   *       ({@code _meta}) or the per-PS meta file ({@code psmeta}). The caller can hand
   *       this directly to {@link com.tencent.angel.model.output.format.MatrixFormatImpl#load}.</li>
   *   <li><b>Returns null</b>: there is genuinely no snapshot to load — either the
   *       per-PS base directory does not exist, or the matrix sub-directory does not
   *       exist, or it exists but contains no integer-named epoch sub-directories.
   *       This is the legitimate "first launch / no previous backup" case; the caller
   *       will fall back to master checkpoints / save-results / InitFunc.</li>
   *   <li><b>Throws {@link IOException}</b>: epoch directories exist but every single
   *       one is empty (no {@code _meta} and no {@code psmeta}). This is a corrupted /
   *       phantom snapshot — almost always the fingerprint of a PS process killed mid-
   *       write by OOM/SIGKILL after {@code mkdir} but before the snapshot dumper's
   *       atomic rename completed. Returning null here would silently demote the
   *       attempt to "matrix served with empty contents" via {@code initMatricesData}'s
   *       InitFunc fall-through, which for algorithms that load already-trained data
   *       (embeddings, GNN weights, …) would silently zero out the model. We instead
   *       fail loudly so the user can inspect / clean up the HDFS state. The recovery
   *       behaviour is identical to the legacy behaviour of "load fails → attempt
   *       fails → job dies after maxAttempts" — but the error surfaces here (clear
   *       phantom-snapshot diagnosis) rather than several frames deeper in the loader
   *       (cryptic "Can not find matrix … in directory …").</li>
   * </ul>
   *
   * <p>Why phantom snapshots happen: {@link com.tencent.angel.ps.io.save.SnapshotDumper#checkpoint}
   * hard-codes {@code checkpointId=0} and writes via tmp-then-rename. If the PS dies
   * between {@code mkdir tmp} and the rename — exactly the OOM-then-restart pattern we
   * hit on PS_22 — the next attempt sees a populated epoch directory layout (the dirs
   * exist) but no actual matrix payload inside.
   *
   * @param matrixId matrix id
   * @return a load-ready snapshot directory, or null if no snapshot exists at all
   * @throws IOException if snapshot epoch directories exist but all are empty (corrupted
   *                     state), or on any underlying HDFS error
   */
  public Path getSnapshotPath(int matrixId) throws IOException {
    String name = context.getMatrixMetaManager().getMatrixMeta(matrixId).getName();
    Path matrixPath = new Path(baseDirPath, name);
    FileSystem fs = matrixPath.getFileSystem(context.getConf());

    // No snapshot dir at all: legitimate "nothing to load", let caller fall back.
    if (!fs.exists(matrixPath)) {
      return null;
    }

    // Collect every entry whose name parses as a non-negative integer. Mis-named
    // siblings (tmp dirs, leftover files) are logged and ignored rather than letting an
    // "Integer.valueOf threw" path silently leak into the candidate set with id=0 — the
    // previous code's behaviour, which was load-bearing for the bug above.
    FileStatus[] status = fs.listStatus(matrixPath);
    if (status == null || status.length == 0) {
      // Directory exists but has nothing in it. Treat as "no snapshot" rather than
      // "corrupted snapshot" — an empty matrix dir most commonly comes from a
      // mkdir-only initialisation, not from a half-written dump.
      return null;
    }
    List<int[]> idAndIndex = new ArrayList<>(status.length);
    for (int i = 0; i < status.length; i++) {
      try {
        int id = Integer.parseInt(status[i].getPath().getName());
        if (id >= 0) {
          idAndIndex.add(new int[]{id, i});
        }
      } catch (NumberFormatException x) {
        LOG.warn("Path " + status[i].getPath().toString()
            + " is not a valid checkpoint epoch directory, ignoring");
      }
    }
    if (idAndIndex.isEmpty()) {
      // Entries exist but none parse as an epoch id — same "no snapshot" semantics.
      return null;
    }

    // Sort by epoch id descending so we try the freshest snapshot first; fall back to
    // older ones only if newer ones turn out to be empty / mid-write.
    idAndIndex.sort((a, b) -> Integer.compare(b[0], a[0]));

    List<Path> skipped = null;
    for (int[] entry : idAndIndex) {
      Path candidate = status[entry[1]].getPath();
      if (isLoadReady(fs, candidate)) {
        return candidate;
      }
      LOG.warn("Snapshot epoch directory " + candidate
          + " exists but contains no usable matrix payload"
          + " (no _meta and no psmeta); skipping and trying older epoch");
      if (skipped == null) {
        skipped = new ArrayList<>(idAndIndex.size());
      }
      skipped.add(candidate);
    }

    // All N epoch directories present but ALL are empty. This is a corrupted /
    // phantom snapshot, NOT a "nothing to load" scenario — the snapshot bookkeeping
    // is asserting "I have a snapshot here" while the payload is missing. Refuse to
    // silently demote to an empty matrix; fail this attempt so the user finds out
    // immediately. The dedicated CorruptedSnapshotException is so that the caller
    // (ParameterServer.initMatricesData, which today swallows generic IOException
    // and silently falls through to the InitFunc path) can rethrow it specifically.
    String msg = "Snapshot for matrix " + name + " under " + matrixPath
        + " is corrupted: " + idAndIndex.size() + " epoch director"
        + (idAndIndex.size() == 1 ? "y" : "ies") + " present but all are empty"
        + " (no _meta and no psmeta). The previous PS attempt was almost certainly"
        + " killed (e.g. OOM / SIGKILL) between the snapshot tmp-write and the atomic"
        + " rename, leaving the directory layout populated but the actual payload"
        + " missing. Inspect / clean up " + matrixPath + " on HDFS and re-run."
        + " Skipped epochs: " + skipped;
    LOG.error(msg);
    throw new CorruptedSnapshotException(msg);
  }

  /**
   * A snapshot directory is "load-ready" iff it carries either the matrix-level meta
   * file ({@link ModelFilesConstent#modelMetaFileName}) or the per-PS meta file
   * ({@link ModelFilesConstent#psModelMetaFileName}). Those are the two markers that
   * {@link com.tencent.angel.model.output.format.MatrixFormatImpl#load} probes before
   * descending into partition files, so any directory missing both is guaranteed to
   * make the loader fail — and we'd rather discover that here and try an older epoch
   * than let the loader throw and kill the whole PS attempt.
   *
   * <p>Best-effort: any HDFS exception during the probe is treated as "not ready" and
   * logged. The caller will then try the next older epoch (or return null).
   */
  private boolean isLoadReady(FileSystem fs, Path epochDir) {
    try {
      Path metaPath = new Path(epochDir, ModelFilesConstent.modelMetaFileName);
      if (fs.exists(metaPath)) {
        return true;
      }
      Path psMetaPath = new Path(epochDir, ModelFilesConstent.psModelMetaFileName);
      if (fs.exists(psMetaPath)) {
        return true;
      }
      return false;
    } catch (IOException e) {
      LOG.warn("Probe load-readiness of " + epochDir + " failed; treating as not ready", e);
      return false;
    }
  }
}
