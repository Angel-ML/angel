package com.tencent.angel.spark.ml.ann

object DistanceFunctions {
  
  class FloatCosineDistance extends DistanceFunction {
    
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      
      var dot: Float = 0.0f
      var nru: Float = 0.0f
      var nrv: Float = 0.0f
      
      for (i <- u.indices) {
        dot += u(i) * v(i)
        nru += u(i) * u(i)
        nrv += v(i) * v(i)
      }
      
      val similarity = dot / (Math.sqrt(nru) * Math.sqrt(nrv))
      (1 - similarity).toFloat
    }
    
    override def getName: String = "cosine-distance"
    
  }
  
  class FloatL1Distance extends DistanceFunction {
    
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      var sum: Float = 0.0f
      
      for (i <- u.indices) {
        val dp = Math.abs(u(i) - v(i))
        sum += dp
      }
      
      sum
    }
    
    override def getName: String = "l1-distance"
    
  }
  
  class FloatL2Distance extends DistanceFunction {
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      var sum: Float = 0.0f
      
      for (i <- u.indices) {
        val dp = u(i) - v(i)
        sum += (dp * dp)
      }
      
      Math.sqrt(sum).toFloat
    }
    
    override def getName: String = "l2-distance"
  }
  
  class JaccardDistance extends DistanceFunction {
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      val sim = u.count(t => v.contains(t)).toFloat / (u ++ v).distinct.length.toFloat
      1 - sim
    }
    
    override def getName: String = "jaccard-distance"
  }
  
  class JaccardDistanceHot extends DistanceFunction {
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      val (interCnt, unionCnt) = u.zip(v).map{t =>
        if (t._1 == 1 && t._2 == 1) (1, 1)
        else if (t._1 == 1 || t._2 == 1) (0, 1)
        else (0, 0)
      }.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
      1 - (interCnt.toFloat / unionCnt.toFloat)
    }
    
    override def getName: String = "jaccard-distance-hot"
  }
  
  class FloatDotDistance extends DistanceFunction {
    override def distance(u: Array[Float], v: Array[Float]): Float = {
      var dot: Float = 0.0f
      for (i <- u.indices) {
        dot += u(i) * v(i)
      }
      -dot
    }
  
    override def getName: String = "dot-distance"
  }
  
  def str2DistanceFunction(disFunc: String): DistanceFunction = {
    disFunc match {
      case "l1-distance" => new DistanceFunctions.FloatL1Distance
      case "l2-distance" => new DistanceFunctions.FloatL2Distance
      case "cosine-distance" => new DistanceFunctions.FloatCosineDistance
      case "jaccard-distance" => new DistanceFunctions.JaccardDistance
      case "dot-distance" => new DistanceFunctions.FloatDotDistance
      case _ => new DistanceFunctions.FloatL2Distance
    }
  }
  
}
