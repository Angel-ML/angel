 package com.tencent.angel.master.ps;

import java.util.List;

public class CommitEvent extends ParameterServerManagerEvent{
  private final List<Integer> needCommitMatrixIds;
      
  public CommitEvent(List<Integer> needCommitMatrixIds) {
    super(ParameterServerManagerEventType.COMMIT);
    this.needCommitMatrixIds = needCommitMatrixIds;
  }
 
  public List<Integer> getNeedCommitMatrixIds() {
    return needCommitMatrixIds;
  }
}
