# Angel的代码框架

---

Angel的代码结构，从整体上可以划分为几大模块：

![][1]


  [1]: ../img/code_framework.png
  

## 1. Angle-Core（核心层）

## 2. Angel-ML（机器学习层）

## 3. Angel-Client（接口层）

Angel的接口层，是可以基于底层，进行插拔式扩展的。目前有2套API，一套是Angel的，一套是Spark的。

* **Angel的API**

	Angel的本身API，设计时关注点在于PSModel。整体的任务运行，有第一代MR的影子，简单而直接。MLRunner通过Task驱动，调用Learner，学习到多个或者1个PSModel组成的MLModel。

	开发稍微有些繁琐，但是整体性能很高，因为没有不必要的开销。详细的介绍在这里：

* **Spark on Angel的API**
	Spark on Angel的API，重点在于和Spark的配合。Angel的PS-Service设计，使得我们可以轻松开发Spark on Angel的API，和Angel类似。通过SparkPSModel，SparkPSClient，SparkPSContext这3个类，轻松的在Spark中，无需修改Core，而直接调用Spark on Angel。
	
	关于Spark on Angel的详细介绍，可以参考这里：

## 4. Angle-MLLib（算法层）


