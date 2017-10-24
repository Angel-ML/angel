package com.tencent.angel.spark.models

import com.tencent.angel.spark.client.PSClient

trait PSModel extends Serializable {
  @transient private[models] lazy val psClient = PSClient.instance()
}
