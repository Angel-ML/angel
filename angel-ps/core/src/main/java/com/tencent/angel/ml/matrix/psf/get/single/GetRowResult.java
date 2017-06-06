package com.tencent.angel.ml.matrix.psf.get.single;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;

/**
 * The final result return to user of the get row function.
 */
public class GetRowResult extends GetResult {
  /** matrix row */
  private final TVector row;

  /**
   * Create a new GetRowResult.
   *
   * @param type response type
   * @param row matrix row
   */
  public GetRowResult(ResponseType type, TVector row) {
    super(type);
    this.row = row;
  }

  /**
   * Get matrix row
   *
   * @return TVector matrix row
   */
  public TVector getRow() {
    return row;
  }
}
