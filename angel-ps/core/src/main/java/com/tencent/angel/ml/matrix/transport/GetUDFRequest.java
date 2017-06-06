package com.tencent.angel.ml.matrix.transport;

import io.netty.buffer.ByteBuf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * Get udf rpc request.
 */
public class GetUDFRequest extends PartitionRequest {
  private static final Log LOG = LogFactory.getLog(GetUDFRequest.class);

  /** the udf class name */
  private String getFuncClass;

  /** partition parameter of the udf */
  private PartitionGetParam partParam;

  /**
   * Create a new GetUDFRequest.
   *
   * @param serverId parameter server id
   * @param partKey matrix partition key
   * @param getFuncClass udf class name
   * @param partParam partition parameter of the udf
   */
  public GetUDFRequest(ParameterServerId serverId, PartitionKey partKey, String getFuncClass,
      PartitionGetParam partParam) {
    super(serverId, 0, partKey);
    this.getFuncClass = getFuncClass;
    this.partParam = partParam;
  }

  /**
   * Create a new GetUDFRequest.
   */
  public GetUDFRequest() {
    this(null, null, null, null);
  }

  @Override
  public int getEstimizeDataSize() {
    MatrixMeta meta =
        PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * (partKey.getEndCol() - partKey.getStartCol());

        case T_INT_DENSE:
          return 4 * (partKey.getEndCol() - partKey.getStartCol());

        case T_FLOAT_DENSE:
          return 4 * (partKey.getEndCol() - partKey.getStartCol());

        case T_DOUBLE_SPARSE:
        case T_INT_SPARSE: {
          return 0;
        }

        default:
          return 8 * (partKey.getEndCol() - partKey.getStartCol());
      }
    }
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.GET_UDF;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (getFuncClass != null) {
      byte[] data = getFuncClass.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
    }

    if (partParam != null) {
      String partParamClassName = partParam.getClass().getName();
      byte[] data = partParamClassName.getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      partParam.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      getFuncClass = new String(data);
    }

    if (buf.isReadable()) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String partParamClassName = new String(data);
      try {
        partParam = (PartitionGetParam) Class.forName(partParamClassName).newInstance();
        partParam.deserialize(buf);
      } catch (Exception e) {
        LOG.fatal("deserialize PartitionAggrParam falied, ", e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int size = super.bufferLen();
    if (getFuncClass != null) {
      size += 4;
      size += getFuncClass.toCharArray().length;
    }

    if (partParam != null) {
      size += 4;
      size += partParam.bufferLen();
    }

    return size;
  }

  /**
   * Get udf class name.
   * 
   * @return String udf class name
   */
  public String getGetFuncClass() {
    return getFuncClass;
  }

  /**
   * Get the partition parameter of the udf.
   * 
   * @return PartitionGetParam the partition parameter of the udf
   */
  public PartitionGetParam getPartParam() {
    return partParam;
  }

  @Override
  public String toString() {
    return "GetUDFRequest [getFuncClass=" + getFuncClass + ", partParam=" + partParam + "]";
  }
}
