package com.tencent.angel.graph.embedding.struc2vec
import scala.collection.mutable.ArrayBuffer

class StructureSimilarity(adjMatrix:Array[Array[Int]]) {
      val INF = Floyd.INF
      val n = adjMatrix.length
      val degrees:Array[Int] = Array.ofDim(n)
      val hopCountResult = Floyd.hopCount(adjMatrix)
      val diam:Int = initDegreesAndDiam()
      val structSimi:Array[Array[Array[Double]]] = Array.ofDim(diam+1,n,n)

      def initDegreesAndDiam(): Int ={
        var diam = 0
        for(i<- 0 until n;j<-0 until n){
          if(hopCountResult(i)(j) == 1)
              degrees(i)+=1

          // Considering that multiple connected component may exist

          if(hopCountResult(i)(j) != INF)
             diam = Math.max(diam,hopCountResult(i)(j))
        }
        diam
      }

      def compute() ={
          for(k<- 0 to diam)
              computeLayerK(k)
      }

      def computeLayerK(k:Int): Unit ={
        if(k<0 || k>diam){

        }else if(k==0){
              for(i<- 0 until n; j<- 0 until n)
                structSimi(0)(i)(j) = DTW.compute(getHopRingK(i,0),getHopRingK(j,0))
        }else{
              for(i<-0 until n; j<- 0 until n){
                // notice that NaN == NaN (false!)
                if (structSimi(k-1)(i)(j).isNaN)
                   structSimi(k)(i)(j) = Double.NaN
                else{
                  val temp: Double = DTW.compute(getHopRingK(i,k),getHopRingK(j,k))
                  if(temp.isNaN)
                    structSimi(k)(i)(j) = Double.NaN
                  else
                    structSimi(k)(i)(j) = structSimi(k-1)(i)(j)+temp
                }
              }
        }
      }

     def getHopRingK(node:Int,k:Int): Array[Int] = {
       // return the ordered degree array s(R_k(node))
       if (node < 0 || node > n - 1 || k < 0 || k > diam)
           Array[Int]()
       else{
         val result = ArrayBuffer[Int]()
         for(j<- 0 until n)
           if(hopCountResult(node)(j)==k)
              result.append(degrees(j))
         result.sorted.toArray
       }
     }

}
