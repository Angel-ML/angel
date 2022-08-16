package com.tencent.angel.graph.embedding.struc2vec

object DTW {
  def compute(a:Array[Int],b:Array[Int]):Double = {

    // DTW(Dynamic Time Warping)
    // a fundamental implementation based on dynamic programming
    // dp[i][j] = Dist(i,j) + min{dp[i-1][j],dp[i][j-1],dp[i-1][j-1]}

    if(a.length==0||b.length==0)
      Double.NaN
    else{
      val dp: Array[Array[Double]] = Array.ofDim(a.length, b.length)

      // set the boundary condition
      // the first column, dp[i][0] = Dist(i,0)+ dp[i-1][0];

      dp(0)(0) = dist(a(0),b(0))
      for(i <- 1 until a.length)
        dp(i)(0) = dp(i-1)(0)+dist(a(i),b(0))

      // the first row, dp[0][j] = Dist(0,j)+ dp[0][j-1];

      for (j <- 1 until b.length)
        dp(0)(j) = dp(0)(j - 1) + dist(b(j), a(0))

      // fill up the whole dp matrix from left to right, from top to bottom

      for(i<-1 until a.length;j<- 1 until b.length)
         dp(i)(j) = dist(a(i),b(j))+minAmongThree(dp(i-1)(j),dp(i)(j-1),dp(i-1)(j-1))

      dp(a.length-1)(b.length-1)
    }
  }

  def dist(a:Int,b:Int):Double = {
    // Assume that a, b are both positive (the node degrees )
    Math.max(a,b)/Math.min(a,b)-1
  }

  def minAmongThree(a:Double,b:Double,c:Double):Double = {
     if(a<b){
       Math.min(a,c)
     }else
       Math.min(b,c)
  }



}
