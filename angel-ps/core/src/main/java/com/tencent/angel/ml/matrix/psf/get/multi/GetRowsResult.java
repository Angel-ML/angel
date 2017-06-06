package com.tencent.angel.ml.matrix.psf.get.multi;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;

import java.util.Map;

/**
 * The final result return to user of the get rows function.
 */
public class GetRowsResult extends GetResult {
  /** matrix row index to row map */
  private final Map<Integer, TVector> rows;

  /**
   * Create a new GetRowsResult.
   *
   * @param responseType response type
   * @param rows matrix rows
   */
  public GetRowsResult(ResponseType responseType, Map<Integer, TVector> rows) {
    super(responseType);
    this.rows = rows;
  }

  /**
   * Get rows.
   *
   * @return Map<Integer, TVector> rows
   */
  public Map<Integer, TVector> getRows() {
    return rows;
  }
}
