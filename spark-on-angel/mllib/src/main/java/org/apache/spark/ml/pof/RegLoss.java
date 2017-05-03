package org.apache.spark.ml.pof;

import com.tencent.angel.spark.func.Zip2MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class RegLoss implements Zip2MapWithIndexFunc {

  private int interceptIndex;
  private boolean fitIntercept;
  private boolean standardization;

  public RegLoss(int interceptIndex, boolean fitIntercept, boolean standardization) {
    this.interceptIndex = interceptIndex;
    this.fitIntercept = fitIntercept;
    this.standardization = standardization;
  }

  public RegLoss() {
    super();
  }

  @Override
  public double call(int index, double coefficient, double featStd) {

    boolean isIntercept = (fitIntercept) && (index > interceptIndex);

    double loss = 0.0;
    if (!isIntercept) {
      if (standardization || featStd == 0.0) {
        loss = coefficient * coefficient;
      } else {
        loss = coefficient * coefficient / (featStd * featStd);
      }
    }
    return loss;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(interceptIndex);
    buf.writeBoolean(fitIntercept);
    buf.writeBoolean(standardization);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    interceptIndex = buf.readInt();
    fitIntercept = buf.readBoolean();
    standardization = buf.readBoolean();
  }

  @Override
  public int bufferLen() {
    return 4 + 1 + 1;
  }
}
