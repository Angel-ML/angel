# Angel的设计理念

----


Angel的整体设计理念，是`简约而不简单`，做一个灵活而强大的参数服务器，并在此之上，提供多种机器学习算法，和PS服务，扩展为一个分布式机器学习平台。

因此，在开发之时，Angel从如下5个方向，对整体进行了改进和设计，并在它们之间进行了平衡。包括：

3. 易用性
1. 灵活性
5. 性能
2. 可扩展性
4. 稳定性

另外，Angel设计之初，并没有特地为深度学习量身定做，但随着深度学习日趋流行，而PS模式，也是深度学习实现多机多卡的方向之一，Angel后续会加入更多对深度学习的针对性优化。

## 灵活性

Angel在设计上，考虑到现有的机器学习框架众多的问题，提供了2种模式。Angel支持两种运行模式：`ANGEL_PS_WORKER` & `ANGEL_PS_SERVICE`。通过这2种方式结合，Angel本身的Worker模式，追求最大化的性能优势，主打速度。而PS-Service模式，主打对接，可以接入Spark，TensorFlow，Torch等平台，主打生态，从而实现最大程度的灵活性。


* **ANGEL_PS_WORKER**：启动Master，PS和Worker，Angel独立完成模型的训练。

* **ANGEL_PS_SERVICE**: PS Service模式，在这种模式下，Angel只启动Master和PS，具体的计算交给其他计算平台（如Spark，TensorFlow）负责，Angel只负责提供Parameter Server的功能。




## 性能

2. **同步协议**

	* 支持多种同步协议：除了通用的**BSP**（Bulk Synchronous Parallel）外，为了解决task之间互相等待的问题，Angel还支持**SSP**（Stale Synchronous Parallel）和**ASP**（Asynchronous Parallel）

## 易用性
* **训练数据和模型自动切割**：Angel根据配置的worker和task数量，自动对训练数据进行切分；同样，也会根据模型大小和PS实例数量，对模型实现自动分区。
* **易用的编程接口**：MLModel/PSModel/AngelClient


## 扩展性

* **psFunc**：为了满足各类算法对参数服务器的特殊需求，Angel将参数获取和更新过程进行了抽象，提供了psf函数功能。用户只需要继承Angel提供的psf函数接口，并实现自己的参数获取/更新逻辑，就可以在不修改Angel自身代码的情况下定制自己想要的参数服务器的接口。

* **自定义数据格式**：Angel支持Hadoop的InputFormat接口，可以方便的实现自定义文件格式。

* **自定义模型切分方式**：默认情况下，Angel将模型（矩阵）切分成大小相等的矩形区域；用户也可以自定义分区类来实现自己的切分方式。


## 稳定

Angel在稳定性上，做了大量的工作，保证在机器学习过程中，单个Worker和PS挂掉，都不会影响整个作业的进度，而且能最快的找到备用的机器，快速启动，加载模型或者数据，继续训练过程。


* **PS容错**

	PS容错采用了checkpoint的模式，也就是每隔一段时间将PS承载的参数分区写到hdfs上去。如果一个PS实例挂掉，Master会新启动一个PS实例，新启动的PS实例会加载挂掉PS实例写的最近的一个checkpoint，然后重新开始服务。这种方案的优点是简单，借助了hdfs 多副本容灾， 而缺点就是不可避免的会丢失少量参数更新。

* **Worker容错**

	一个Worker实例挂掉后，Master会重新启动一个Worker实例，新启动的Worker实例从Master处获取当前迭代轮数等状态信息，从PS处获取最新模型参数，然后重新开始被断掉的迭代。

* **Master容错**

	Master定期将任务状态写入HDFS，借助与Yarn提供的App Master重试机制，当Angel的Master挂掉后，Yarn会重新拉起一个Angel的Master，新的Master加载状态信息，然后重新启动Worker和PS，从断点出重新开始计算。

* **慢Worker检测**

	Master会将收集一些Worker计算性能的一些指标，如果检测到有一些Worker计算明显慢于平均计算速度，Master会将这些Worker重新调度到其他的机器上，避免这些Worker拖慢整个任务的计算进度。
