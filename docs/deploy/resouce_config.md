# Angel资源配置

---
当我们运行Angel任务之前，首先需要确定任务的资源参数：主要包括Master的内存大小，Master的CPU vcore数，Worker的个数，单个Worker的内存大小，单个Worker的CPU vcore数， PS个数，单个PS内存大小， 单个PS需要的CPU vcore数等。

由于Master比较轻量，一般使用默认参数就可以满足要求了，所以我们主要需要配置的是Worker和PS的资源参数。 为了尽量合理的配置资源，不可避免的需要了解一些Angel组件的资源使用情况，因此下面的部分会首先介绍Worker和PS的内存使用情况和基本的估算方法。值得注意的是，由于算法和算法不同实现差别很大，所以这里给出的方法通用性有限，需要结合具体算法来分析。本文档的后续部分会给出每一种算法的资源配置参考。

## **Worker资源预估**
### **Worker个数预估**
为了更好的实现数据并行，Angel可以自由配置Worker的个数。一般情况下，Worker的个数主要取决于总的需要计算的数据量大小。一般的，单个Worker处理的数据量推荐值为**1~5GB**（这里特指未压缩的文本格式数据，其他格式需要乘以对应的压缩比，下同）。

### **Worker内存预估**
Worker的内存使用状况如图所示：
	![][1]
 - **模型（model）**：从PS拉取的当前Worker需要计算的模型部分
 - **模型更新（model delta）**：Worker的Task计算得到的模型更新
 - **合并后的模型更新（merged model delta）**：合并后的模型更新
 - **系统(system buffer)**：Netty框架使用的ByteBuf pool等
 - **格式化后的训练数据(training data)**：原始的训练数存储在分布式文件系统之上（例如HDFS），在模型训练开始之前，Worker会将分配给自己的训练数据从分布式文件系统上拉取回本地，经过一些格式的转换后存在本地磁盘或者Worker的内存中。为了提高计算性能，建议将格式化后的训练数据存在Worker内存中，原始训练数据大小和格式化后占用内存空间的比率为**1.5：1**，即1.5GB的原始训练数据经格式化并加载到内存中后占用1GB的内存空间（dummy和libsvm格式数据均为1.5：1左右）。当然，如果内存紧张，可以使用本地磁盘来存储格式化的数据
 
定义如下变量：

训练使用的模型部分的大小为`Sm`

task每轮迭代产生的更新大小为`Smd`

合并后的模型更新大小为`Smmd`

训练数据占用的内存大小为`St`

系统部分占用内存大小为`Sb`

一个Worker上运行的Task数量为`N`

则Worker内存估算公式为：

```Me = N * Sm + 2 * N * Smd + Smmd + St + Sb```

系统使用的内存可以通过下面的方法简单估算：

```Sb = Sm + Smmd + 1```

因此，估算公式可以简化为：

```Me = (N + 1) * Sm + 2 * N * Smd + 2 * Smmd + St + 1```

### **Worker CPU vcore数预估**
与内存参数不一样的是CPU vcore只会影响任务执行效率，而不会影响任务的正确性。建议CPU vcore数和和内存参数按物理机器资源比例来调整。举一个简单的例子：如果一台物理机器总的内存大小是100G，CPU vcore总数为50，当Worker内存配置为10G/20G时，CPU vcore可以配置为5/10。

## **PS资源预估**
### **PS个数预估**
PS个数配置主要和Worker个数和模型大小相关。一般Worker数量越多，模型越大，需要的PS个数越多。推荐PS个数为Worker个数的1/5~4/5。PS个数和算法相关性非常大，应具体算法具体分析。
### **PS内存预估**
PS的内存使用如图所示：
	![][2]
 - **模型分区（model partitions）**：每个PS会承担模型的一部分
 - **系统（system buffer）**：Netty框架使用的ByteBuf pool等

由于要与多个Worker发生大数据量的交互，需要大量的发送和接收缓冲区。因此，一般情况下系统本身消耗的内存远大于模型分区本身。

定义如下变量：

模型分区大小为`Smp`

Task每轮迭代需要获取的模型部分大小为`Sw`

Worker个数为`N`

可以通过下面方式简单估算PS所需内存：

```Smp + 2 * N * Sw```

### **PS CPU vcore个数预估**
PS CPU vcore个数估算方法与Worker一样。

# Logistic Regression参数配置举例
默认情况下，LR的模型是一个稠密的，Angel内部以double数组来存储模型和模型的分区。 因此对于Worker内存， `Sm = Smd = Smmd`。
假设LR的训练数据为300GB， 模型维度为1亿， 则可以配置100个Worker，每个Worker执行的Task数量为1，每个Worker需要的内存估算如下：
```
Sm = Smd = Smmd = 8 * 10 ^ 8 / 10 ^ 9 = 0.8GB
St = 300 / 100 / 1.5 = 2GB
N = 1
Me = (N + 1) * Sm + 2 * N * Smd + 2 * Smmd + St + 1 = 2 * 0.8 + 2 * 0.8 + 2 * 0.8 + 2 + 1 = 7.8GB
Me 向上取整为8GB
```
PS个数为Worker个数的1/5， 即20个，一个PS上承载的模型分区大小为500万， 每个PS需要的内存为：
```
Smp = Sw = 8 * 5 * 10 ^ 6 / 10 ^ 9 = 0.04GB
Me = 0.04 + 2 * 100 * 0.04 = 8.04GB
Me约为8G
```

假设每一台物理机器内存为128GB, CPU vcore总数为48个， 则Worker的CPU vcore数为：
```
Vw = 48 / 128 * 8 = 3
```
PS 的CPU vcore数为：
```
Vp = 48 / 128 * 8 = 3
```

 [1]: ../img/worker_memory.png
 [2]: ../img/ps_memory.png

