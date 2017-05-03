/**  
 * @Description: TODO(用一句话描述该文件做什么)
*/
    
package com.tencent.angel.example;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.model.AlgorithmModel;
import com.tencent.angel.worker.predict.PredictResult;
import com.tencent.angel.worker.storage.Storage;

public class GetRowModel extends AlgorithmModel {

  /* (非 Javadoc)
  * 
  * 
  * @param storage
  * @return
  * @see com.tencent.angel.ml.model.AlgorithmModel#predict(com.tencent.angel.worker.storage.Storage)
  */
      
  @Override
  public Storage<PredictResult> predict(Storage<LabeledData> storage) {
    // TODO Auto-generated method stub
    return null;
  }

}
