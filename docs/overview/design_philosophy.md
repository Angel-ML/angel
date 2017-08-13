# Angel的设计理念


## PS Service

Angel支持两种运行模式：`ANGEL_PS` & `ANGEL_PS_WORKER`

* **ANGEL_PS**: PS Service模式，在这种模式下，Angel只启动Master和PS，具体的计算交给其他计算平台（如Spark，Tensorflow）负责，Angel只负责提供Parameter Server的功能。

* **ANGEL_PS_WORKER**：启动Master，PS和Worker，Angel独立完成模型的训练。

## 同步协议

* 支持多种同步协议：除了通用的**BSP**（Bulk Synchronous Parallel）外，为了解决task之间互相等待的问题，Angel还支持**SSP**（Stale Synchronous Parallel）和**ASP**（Asynchronous Parallel）

## 良好的可扩展性

* **psf(ps function)**：为了满足各类算法对参数服务器的特殊需求，Angel将参数获取和更新过程进行了抽象，提供了psf函数功能。用户只需要继承Angel提供的psf函数接口，并实现自己的参数获取/更新逻辑，就可以在不修改Angel自身代码的情况下定制自己想要的参数服务器的接口。

* **自定义数据格式**：Angel支持Hadoop的InputFormat接口，可以方便的实现自定义文件格式。

* **自定义模型切分方式**：默认情况下，Angel将模型（矩阵）切分成大小相等的矩形区域；用户也可以自定义分区类来实现自己的切分方式。

## 易用性
* **训练数据和模型自动切割**：Angel根据配置的worker和task数量，自动对训练数据进行切分；同样，也会根据模型大小和PS实例数量，对模型实现自动分区。
* **易用的编程接口**：MLModel/PSModel/AngelClient

## 容错设计和稳定性

* **PS容错**

	PS容错采用了checkpoint的模式，也就是每隔一段时间将PS承载的参数分区写到hdfs上去。如果一个PS实例挂掉，Master会新启动一个PS实例，新启动的PS实例会加载挂掉PS实例写的最近的一个checkpoint，然后重新开始服务。这种方案的优点是简单，借助了hdfs 多副本容灾， 而缺点就是不可避免的会丢失少量参数更新。

* **Worker容错**

	一个Worker实例挂掉后，Master会重新启动一个Worker实例，新启动的Worker实例从Master处获取当前迭代轮数等状态信息，从PS处获取最新模型参数，然后重新开始被断掉的迭代。

* **Master容错**

	Master定期将任务状态写入hdfs，借助与Yarn提供的App Master重试机制，当Angel的Master挂掉后，Yarn会重新拉起一个Angel的Master，新的Master加载状态信息，然后重新启动Worker和PS，从断点出重新开始计算。

* **慢Worker检测**

	Master会将收集一些Worker计算性能的一些指标，如果检测到有一些Worker计算明显慢于平均计算速度，Master会将这些Worker重新调度到其他的机器上，避免这些Worker拖慢整个任务的计算进度。
