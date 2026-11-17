package com.tencent.angel.spark.ml.psf.louvainDensity.GetCommunityInfor;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor.CommunityInforElement;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class PartGetCommunityInforResult extends PartitionGetResult {

  private int partId;
  private CommunityInforElement[] communitiesInfor;

  public PartGetCommunityInforResult(int partitionId, CommunityInforElement[] communitiesInfor) {
    this.partId = partitionId;
    this.communitiesInfor = communitiesInfor;
    assert (communitiesInfor != null);
  }

  public PartGetCommunityInforResult() {
    this(-1, null);
  }


  public CommunityInforElement[] getCommunitiesInfor() {
    return communitiesInfor;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(communitiesInfor.length);
    for (int i = 0; i < communitiesInfor.length; i++) {
      if (communitiesInfor[i] == null) {
        output.writeLong(0);
        output.writeLong(0);
        output.writeFloat(0);
        output.writeFloat(0);
      } else {
        communitiesInfor[i].serialize(output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int len = input.readInt();
    communitiesInfor = new CommunityInforElement[len];

    for (int i = 0; i < len; i++) {
      CommunityInforElement elem = new CommunityInforElement();
      elem.deserialize(input);
      communitiesInfor[i] = elem;
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < communitiesInfor.length; i++) {
      if (communitiesInfor[i] == null) {
        len += Long.BYTES * 2 + Float.BYTES * 2;
      } else {
        len += communitiesInfor[i].bufferLen();
      }
    }
    return len;
  }
}
