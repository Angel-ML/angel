# Task 执行流程
Task是Angel的最小计算单元，一个Task的执行流程如图所示：

![][1]

基本流程主要有两步：

 1. 训练数据准备。原始的数据存在分布式文件系统之上，且格式一般不能直接被机器学习算法使用。所以Angel抽象出了训练数据准备这一过程：在这个过程中，Task将分布式文件系统上的数据拉取到本地，然后解析并转换成所需的数据结构。
 2. 计算。对于一般的模型训练，这一步会进行多轮的迭代计算，最后输出一个模型；对于预测，数据只会被计算一次，输出预测结果。
 
 为了让应用程序定制所需的计算流程，Angel抽象出了BaseTaskInterface接口，并在其基础上提供BaseTask，TrainTask和PredictTask等基类。应用程序可以根据自己的需求扩展这些基类。

# BaseTaskInterface<KEYIN, VALUEIN, VALUEOUT>
BaseTaskInterface定义了一个算法的计算流程接口。KEYIN和VALUEIN表示原始输入数据的类型；VALUEOUT表示预处理输出的数据类型，同时它也是训练过程的输入数据。

## parse
- 定义：```VALUEOUT parse(KEYIN key, VALUEIN value)```


- 功能描述：解析原始输入数据的一行，生成训练过程需要的数据结构


- 参数：KEYIN key, VALUEIN value 以键值对表示的原始数据


- 返回值：VALUEOUT  训练数据

## preProcess
- 定义：```void preProcess(TaskContext taskContext)```


- 功能描述：表示从原始数据块到训练数据集合的转换过程


- 参数：TaskContext taskContext 运行预处理过程的Task相关信息


- 返回值：无

## run
- 定义：```void run(TaskContext taskContext) throws AngelException```


- 功能描述：模型训练过程


- 参数：TaskContext taskContext 运行模型训练过程的Task相关信息， 关于TaskContext详情可参考 [TaskContext]()


- 返回值：无

下图是一个简单的数据处理和计算流程


在一般情况下，数据转换都是逐条进行的，因此Angel提供了一个实现了默认按行处理的preProcess函数的类BaseTask。

在很多算法中，需要的训练数据都是一种带有标签的数据结构，Angel定义了一个数据结构[LabeledData]() 表示这一类数据结构。为了进一步简化应用程序编程接口，Angel定义了两个BaseTask的子类TrainTask和PredictTask（这两个类的VALUEOUT均为LabeledData），分别用于训练和预测模式下。应用程序可以根据需求扩展TrainTask和PredictTask。

# TrainTask[KEYIN, VALUEIN]
KEYIN和VALUEIN含义可参考BaseTaskInterface

## train
- 定义：```void train(TaskContext taskContext)```


- 功能描述：模型训练过程


- 参数：TaskContext taskContext Task相关信息


- 返回值：无

# PredictTask[KEYIN, VALUEIN]
KEYIN和VALUEIN含义可参考BaseTaskInterface

## train
- 定义：```def predict(taskContext: TaskContext)```


- 功能描述：利用模型进行预测


- 参数：TaskContext taskContext Task相关信息


- 返回值：无


  [1]: ../img/Task%E6%89%A7%E8%A1%8C%E6%B5%81%E7%A8%8B.png "Task执行流程"
