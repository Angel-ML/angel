# Task

--

> Task是Angel的元计算类，所有的机器学习算法，都要通过继承它，来实现训练或者预测过程。它运行于Worker之内，Task可以共享一个Worker中的某些资源。
 
理解Task，对了解Angel编程原则有很重要的帮助

* Task中，完成对数据的读取和训练两个动作，一个Task只负责它自己读取到的数据的训练
* 中间结果不落地，不对外界开放，这个和其它大部分系统都不一样
* 不同的Task，计算时不互相传输数据，它们都只和PSServer打交道

## 功能

一个Task的执行流程如图所示：

![][1]

Task基本流程主要有2步：

1. **训练数据读取**
	
	原始的数据存在分布式文件系统之上，且格式一般不能直接被机器学习算法使用。所以Angel抽象出了训练数据准备这一过程：在这个过程中，Task将分布式文件系统上的数据拉取到本地，然后解析并转换成所需的数据结构，放入DataBlock之中。这一步包括了preProcess和parse
 
2. **计算（训练 or 预测）**
	
	对于一般的模型训练，这一步会进行多轮的迭代计算，最后输出一个模型；对于预测，数据只会被计算一次，输出预测结果。这一步一般叫run
 

 为了让应用程序定制所需的计算流程，Angel抽象出了**BaseTaskInterface**接口，并在其基础上提供**BaseTask**，**TrainTask**和**PredictTask**等基类。应用程序可以根据自己的需求扩展这些基类。

Task在计算过程中，需要用到一些系统配置信息和控制迭代进度等，这些是通过**TaskContext**来提供的

## 核心类

### **BaseTaskInterface<KEYIN, VALUEIN, VALUEOUT>**

BaseTaskInterface定义了一个算法的计算流程接口。KEYIN和VALUEIN表示原始输入数据的类型；VALUEOUT表示预处理输出的数据类型，同时它也是训练过程的输入数据。

1. **parse**
	- 定义：```VALUEOUT parse(KEYIN key, VALUEIN value)```
	- 功能描述：解析原始输入数据的一行，生成训练过程需要的数据结构
	- 参数：KEYIN key, VALUEIN value 以键值对表示的原始数据
	- 返回值：VALUEOUT  训练数据

2. **preProcess**

	- 定义：```void preProcess(TaskContext taskContext)```
	- 功能描述：表示从原始数据块到训练数据集合的转换过程
	- 参数：TaskContext taskContext 运行预处理过程的Task相关信息
	- 返回值：无

3. **run**

	- 定义：```void run(TaskContext taskContext) throws AngelException```
	- 功能描述：模型训练过程
	- 参数：TaskContext taskContext 当前Task相关信息
	- 返回值：无

为了进一步简化应用程序编程接口，Angel定义了两个BaseTask的子类TrainTask和PredictTask（这两个类的VALUEOUT均为LabeledData），分别用于训练和预测模式下。应用程序可以根据需求扩展TrainTask和PredictTask。

### TrainTask[KEYIN, VALUEIN]

1.  **train**
	- 定义：```void train(TaskContext taskContext)```
	- 功能描述：模型训练过程
	- 参数：TaskContext taskContext Task相关信息
	- 返回值：无

### PredictTask[KEYIN, VALUEIN]

1. **predict**

	- 定义：```def predict(taskContext: TaskContext)```
	- 功能描述：利用模型进行预测
	-  参数：TaskContext taskContext 当前Task相关信息
	- 返回值：无

### TaskContext

应用程序可以通过TaskContext来获取任务配置，Task运行信息等。除此之外，也可以将计算过程中的一些指标保存在TaskContext中以便任务页面展示。

1. **getReader**
	- 定义：```<K, V> Reader<K, V> getReader()```
	- 功能描述：获取分配给这个Task的数据块对应的Reader，一般只在preProcess接口中使用
	- 参数：无
	- 返回值：原始数据块Reader

2. **getConf**
	- 定义：```Configuration getConf()```
	- 功能描述：获取任务配置信息
	- 参数：无
	- 返回值：任务配置

3. **getTotalTaskNum**
	- 定义：```int getTotalTaskNum()```
	- 功能描述：获取任务总的Task数量
	- 参数：无
	- 返回值：任务总的Task数量

4.  **getIteration**
	- 定义：```int getIteration()```
	- 功能描述：获取当前Task的迭代轮数
	- 参数：无
	- 返回值：当前Task迭代到多少轮
	
5. **incIteration**
	- 定义：```void incIteration()```
	- 功能描述：将迭代轮数加1，表示进入下一轮迭代
	- 参数：无
	- 返回值：无

6. **getMatrixClock**
	- 定义：```int getMatrixClock(int matrixId)```
	- 功能描述：获取某个矩阵的时钟信息。时钟信息用于进行Task之间的同步控制
	- 参数：int matrixId 矩阵id
	- 返回值：矩阵的当前时钟

7. **globalSync**

	- 定义：```void globalSync()```
	- 功能描述：将当前Task与任务下的所有其他Task进行同步，让他们进度一致。本方法主要用于BSP同步模型下，一般情况下并不需要应用程序显式调用它来实现同步控制（Angel在内部已经做了同步控制，内部同步过程是以矩阵为单位进行的），除非算法有这方面的特殊需求，否则不需要调用该方法
	- 参数：无
	- 返回值：无

8. **setCounter**
	- 定义：```void setCounter(String counterName, int updateValue)```
	- 功能描述：设置Counter值
	- 参数
		- String counterName Counter名
		- int updateValue Counter值

	- 返回值：无

9. **updateCounter**
	- 定义：```void updateCounter(String counterName, int updateValue)```
	- 功能描述：以累加的方式更新Counter值
	- 参数：String counterName Counter名；int updateValue Counter更新值
	- 返回值：无


  [1]: ../img/task_execute.png
