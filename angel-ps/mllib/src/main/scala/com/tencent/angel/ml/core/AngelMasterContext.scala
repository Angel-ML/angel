package com.tencent.angel.ml.core

import com.tencent.angel.client.AngelClient
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.psagent.PSAgent

case class AngelMasterContext(override val client: AngelClient) extends EnvContext[AngelClient]


case class AngelWorkerContext(override val client: PSAgent) extends EnvContext[PSAgent]
