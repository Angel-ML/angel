package org.apache.spark.ml.pof;

import com.tencent.angel.spark.func.Zip2MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class RegGradient implements Zip2MapWithIndexFunc {

  private double regParam;
  private int interceptIndex;
  private boolean fitIntercept;
  private boolean standardization;
  public RegGradient(
      double regParam, int interceptIndex, boolean fitIntercept, boolean standardization) {
    this.regParam = regParam;
    this.interceptIndex = interceptIndex;
    this.fitIntercept = fitIntercept;
    this.standardization = standardization;
  }

  public RegGradient() {
    super();
  }

  @Override
  public double call(int index, double coeff, double featureStd) {
    boolean isIntercept = index > interceptIndex;
    if (fitIntercept && isIntercept ) {
      return 0.0;
    } else {
      if (standardization || featureStd == 0.0) {
        return coeff * regParam;
      } else {
        return coeff / (featureStd * featureStd) * regParam;
      }
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(regParam);
    buf.writeInt(interceptIndex);
    buf.writeBoolean(fitIntercept);
    buf.writeBoolean(standardization);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    regParam = buf.readDouble();
    interceptIndex = buf.readInt();
    fitIntercept = buf.readBoolean();
    standardization = buf.readBoolean();
  }

  @Override
  public int bufferLen() {
    return 8 + 4 + 1 + 1;
  }
}
