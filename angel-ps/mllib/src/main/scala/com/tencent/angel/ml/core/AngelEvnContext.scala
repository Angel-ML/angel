package com.tencent.angel.ml.core

import com.tencent.angel.client.AngelClient
import com.tencent.angel.ml.core.network.EvnContext

case class AngelEvnContext(angelClient: AngelClient) extends EvnContext
