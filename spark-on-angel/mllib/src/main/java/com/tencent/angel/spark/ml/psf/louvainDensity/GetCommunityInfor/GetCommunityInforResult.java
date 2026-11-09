package com.tencent.angel.spark.ml.psf.louvainDensity.GetCommunityInfor;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor.CommunityInforElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetCommunityInforResult extends GetResult {

  private Long2ObjectOpenHashMap<CommunityInforElement> communityIdeToInfor;

  public GetCommunityInforResult(
      Long2ObjectOpenHashMap<CommunityInforElement> communityIdeToInfor) {
    this.communityIdeToInfor = communityIdeToInfor;
  }

  public Long2ObjectOpenHashMap<CommunityInforElement> getCommunityIdeToInfor() {
    return communityIdeToInfor;
  }

  public void setCommunityIdeToInfor(
      Long2ObjectOpenHashMap<CommunityInforElement> communityIdeToInfor) {
    this.communityIdeToInfor = communityIdeToInfor;
  }

}
