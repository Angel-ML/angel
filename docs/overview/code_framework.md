# Angel的代码框架

---

Angel的代码结构，从整体上可以划分为几大模块：

![][1]


  [1]: ../img/code_framework.png
  

## 1. Angle-Core（核心层）

Angel的核心层，包括了如下核心组件：

* PSServer
* PSAgent
* Worker
* AngelClient
* 网络：RPC & RDMA
* 存储：Memory & Disk
* ……



## 2. Angel-ML（机器学习层）

Angel是面向机器学习的，所以关于机器学习的相关元素，也是加入到Core层的，只不过是单独一个目录

包括了

* Matrix
* Vector
* Feature
* Optimizer
* Objective
* Metric
* psFunc

但是请留意，在Core包中的ML层，大部分都是底层基础接口，而相关的扩展和实现，还是在具体的算法层

## 3. Angel-Client（接口层）

Angel的接口层，是可以基于底层，进行插拔式扩展的。目前有2套API，一套是Angel的，一套是Spark的。

* **Angel的API**

	Angel的本身API，设计时关注点在于PSModel。整体的任务运行，有第一代MR的影子，简单而直接。MLRunner通过Task驱动，调用Learner，学习到多个或者1个PSModel组成的MLModel。

* **Spark on Angel的API**

	Spark on Angel的API，重点在于和Spark的配合。Angel的PS-Service设计，使得我们可以轻松开发Spark on Angel，在Spark中，无需修改Core，而直接调用Spark on Angel。

## 4. Angle-MLLib（算法层）

基于Angel本身的接口，我们开发了多个算法，包括：

* SVM
* LR（各种优化方法）
* KMeans
* GBDT
* LDA
* MF（矩阵分解）
* FM（因式分解机）

Angel的算法开发思路比较直接，都是围绕着PS上的模型进行，不停更新。算法的实现技巧也比较灵活，追求最优的性能。

整体上，通过这4个层级的代码，用户可以全方位的积木式进行代码的设计，开发出高效的机器学习代码。

