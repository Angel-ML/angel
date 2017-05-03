package com.tencent.angel.spark

/**
 * PSVector is a vector store on the PS nodes, and PSVectorProxy is the proxy of PSVector.
 * PSVector has three forms: LocalPSVector, RemotePSVector and BreezePSVector,
 * these three forms of PSVector have implement a set of operations for different situation.
 * LocalPSVector implements the operations for PSVector local form.
 * RemotePSVector implements the operations between PSVector and local data.
 * BreezePSVector implements the operations among PSVectors on PS nodes.
 */
abstract class PSVector extends Serializable {

  def proxy: PSVectorProxy

  def length: Int = proxy.numDimensions

  def size: Int = length

}
