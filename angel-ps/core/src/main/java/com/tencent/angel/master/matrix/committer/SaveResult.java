package com.tencent.angel.master.matrix.committer;

public class SaveResult {

  public SaveResult(String modelPath, String matrixPath, long saveTs) {
    this.modelPath = modelPath;
    this.matrixPath = matrixPath;
    this.saveTs = saveTs;
  }

  private String modelPath;
  private String matrixPath;
  private long saveTs;

  public String getModelPath() {
    return modelPath;
  }

  public void setModelPath(String modelPath) {
    this.modelPath = modelPath;
  }

  public String getMatrixPath() {
    return matrixPath;
  }

  public void setMatrixPath(String matrixPath) {
    this.matrixPath = matrixPath;
  }

  public long getSaveTs() {
    return saveTs;
  }

  public void setSaveTs(long saveTs) {
    this.saveTs = saveTs;
  }
}
