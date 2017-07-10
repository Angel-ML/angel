# Angel的架构设计

---

![][1]

Angel从架构上，分为如下3个大层次：

## Parameter Server层

Parameter Server层中的PSServer，负责分布式存储和更新参数。一个Angel任务的计算过程，会将多个模型，分布式存储于这些PSServer实例中，从而支撑比单机更大的模型，实现模型并行。

## Worker层

Worker层中的Worker，负责具体的模型训练或者结果预测。为了支持更大规模的训练数据，一个Angel任务，包含多个Worker实例，每个Worker实例负责使用一部分训练数据进行训练，实现数据并行。

一个Worker包含1..n个Task，Task是Angel计算单元。同个Worker的Task，可以共享Worker的公共资源。

## 中间层

在Worker和Parameter Server之间，存在非实质性的中间层。负责**异步控制（SyncProtocol)，模型切分（Model Partitioner），自定义函数（psFunc）**。这3者结合，影响具体的机器学习算法的设计，提供多种加速技巧，让算法更加高效。

除了这3大层次之外，还有几个类，也非常重要：

* **Client**

	Angel的客户端，它给应用程序提供了控制任务运行的功能。目前它支持的控制接口主要有：

	* 启动和停止Angel任务
	* 加载和存储模型
	* 启动具体计算过程
	* 获取任务运行状态
	* ……

* **Master**

	Angel的Master，负责统筹Worker和PSServer。其职责主要包括：

	* 原始计算数据以及参数矩阵的分片和分发
	* 向Yarn申请Worker和PSServer所需的计算资源
	* 协调，管理和监控Worker以及PSServer
	* ……


[1]: ../img/angel_architecture_1.png
