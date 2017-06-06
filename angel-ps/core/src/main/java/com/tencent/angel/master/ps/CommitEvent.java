 package com.tencent.angel.master.ps;

import java.util.List;

/**
 * Notice the parameter servers to write matrices to files.
 */
public class CommitEvent extends ParameterServerManagerEvent{
  /**the matrices that need save to files*/
  private final List<Integer> needCommitMatrixIds;
      
  /**
   * Create a CommitEvent
   * @param needCommitMatrixIds the matrices that need save to files
   */
  public CommitEvent(List<Integer> needCommitMatrixIds) {
    super(ParameterServerManagerEventType.COMMIT);
    this.needCommitMatrixIds = needCommitMatrixIds;
  }
 
  /**
   * Get the matrices that need save to files
   * @return the matrices that need save to files
   */
  public List<Integer> getNeedCommitMatrixIds() {
    return needCommitMatrixIds;
  }
}
