package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.network.EnvContext

case class NullClient()

case class LocalEnvContext(override val client: NullClient = null) extends EnvContext[NullClient]
