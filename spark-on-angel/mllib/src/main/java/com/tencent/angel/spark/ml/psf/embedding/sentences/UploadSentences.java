package com.tencent.angel.spark.ml.psf.embedding.sentences;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerSentences;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UploadSentences extends UpdateFunc {

  private static final Log LOG = LogFactory.getLog(UploadSentences.class);

  public UploadSentences(UploadSentencesParam param) {
    super(param);
  }

  public UploadSentences() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof UploadSentencesPartitionParam) {
      UploadSentencesPartitionParam param = (UploadSentencesPartitionParam) partParam;

      if (param.initialize) {
        ServerSentences.initialize(param.numPartitions);
        LOG.info("initialize sentences with numPartition = " + param.numPartitions);
      }

      ServerSentences.batches[param.partitionId] = param.sentences;
      LOG.info("setting sentences with partitionId = " + param.partitionId);
    }
  }
}
