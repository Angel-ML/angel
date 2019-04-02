package com.tencent.angel.ml.core

import com.tencent.angel.client.AngelClient
import com.tencent.angel.ml.core.network.EnvContext

case class AngelEnvContext(override val client: AngelClient) extends EnvContext[AngelClient]
