package com.tencent.angel.ps;

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * Matrix partitioner interface.
 */
public interface Partitioner {
  /**
   * Init matrix partitioner
   * @param mContext matrix context
   * @param conf
   */
  void init(MatrixContext mContext, Configuration conf);

  /**
   * Generate the partitions for the matrix
   * @return the partitions for the matrix
   */
  List<MLProtos.Partition> getPartitions();

  /**
   * Assign a matrix partition to a parameter server
   * @param partId matrix partition id
   * @return parameter server index
   */
  int assignPartToServer(int partId);
}
