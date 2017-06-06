![image.png](/tdw/angel/uploads/A41A5B2D10424BCEB7DB3D22C0FBD653/image.png)


## Client
Angel的客户端，它给应用程序提供了控制任务运行的功能。目前它支持的控制接口主要有：启动和停止Angel任务，加载和存储模型，启动具体计算过程和获取任务运行状态等。
## Master
Master的职责主要包括：原始计算数据以及参数矩阵的分片和分发；向Gaia（一个基于Yarn二次开发的资源调度系统）申请Worker和ParameterServer所需的计算资源； 协调，管理和监控Worker以及ParameterServer。
## Parameter Server
ParameterServer负责存储和更新参数，一个Angel计算任务可以包含多个ParameterServer实例，而整个模型分布式存储于这些ParameterServer实例中，这样可以支撑比单机更大的模型。
## Worker
Worker负责具体的模型训练或者结果预测，为了支持更大规模的训练数据，一个计算任务往往包含许多个Worker实例，每个Worker实例负责使用一部分训练数据进行训练。一个Worker包含一个或者多个Task，Task是Angel计算单元，这样设计的原因是可以让Task共享Worker的许多公共资源。


![image.png](/tdw/angel/uploads/2DE6763E4B3848E4BB0E96ECEB66A076/image.png)

![Angel架构.png](/tdw/angel/uploads/8E16A1D4B9CE4948BC46D8D90469E2B5/Angel架构.png)
