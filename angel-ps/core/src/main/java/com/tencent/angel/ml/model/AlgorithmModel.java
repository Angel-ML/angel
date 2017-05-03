package com.tencent.angel.ml.model;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.worker.predict.PredictResult;
import com.tencent.angel.worker.storage.Storage;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public abstract class AlgorithmModel {
  protected final Map<String, PSModel> psModels;

  public AlgorithmModel() {
    this.psModels  = new HashMap<String, PSModel>();
  }

  public AlgorithmModel(Map<String, PSModel> psModels) {
    this.psModels  = psModels;
  }

  public Map<String, PSModel> getPsModels() {
    return psModels;
  }

  public void addPSModel(String name, PSModel psModel) {
    psModels.put(name, psModel);
  }

  public void addPSModel(PSModel psModel) {
    psModels.put(psModel.getName(), psModel);
  }

  //public abstract <IN, OUT> Storage<OUT> predict(Storage<IN> storage) ;
  public abstract Storage<PredictResult> predict(Storage<LabeledData> storage);

  public void setSavePath(Configuration conf) {

  }

  public void setLoadPath(Configuration conf)
  {

  }
}
