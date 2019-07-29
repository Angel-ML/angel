package com.tencent.angel.decay

import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.optimizer.decayer._
import java.util

import org.scalatest.{BeforeAndAfter, FunSuite}

class DecayTest extends FunSuite with BeforeAndAfter {
    private val eta: Double = 1.0
    private implicit val conf: SharedConf = new SharedConf

    test("standardDecayTest") {
        val decay = new StandardDecay(eta, 0.05)
        calNext(decay)
    }

    test("WarmRestartsTest") {
        val decay = new WarmRestarts(eta, 0.001, 0.05)
        calNext(decay)
    }

    private def calNext(scheduler: StepSizeScheduler) {
        val len: Int = 300
        val data = (0 until len).toArray.map(_ => scheduler.next())
        System.out.println(util.Arrays.toString(data))
    }
}
