# Angel编程指南（Angel Programing Guide）

---

Angel的设计理念，从一开始就围绕`机器学习和模型`。整体上的接口崇尚简约，利用Parameter Server的本质来支持高维度的模型。Angel的主要核心抽象是`PSModel`，它将对分布于多台PS Server上的`远程模型`操作透明化。通过PSModel，用户可以方便的进行**模型的更新，自定义函数计算，以及同步控制**，从而实现各种高效的机器学习算法。


## PSModel和MLModel  

在Angel中，`模型`是一等公民，在Angel中实现一个算法，可以很直白的，从Model Base的思路，来考虑要怎么样设计。

值得留意的是，PSModel是分布式可变的，这种可变性是线程安全，也是对用户透明的。这是Angel和Spark最大的区别，也是它的优势所在。


**PSModel（远程模型）**

PSModel是Angel的核心抽象。它是一个远程模型的概念，对于Client来说，它是一个类似`模型代理`的类。通过它，你可以在每个Worker上，像操作本地对象一样的，去操作一个模型，而实际上，你在操作的是，一个均匀切分在远程多个PSServer上的**分布式模型切片**，而且所有的操作都是透明并发的。

所以，实现一个算法的最小集，需要定义一个PSModel，例如：

```
  val loss = PSModel[DenseDoubleVector]("lr_loss_martrix", 1, epochNum)
```


**MLModel（模型集）**

很多复杂的算法，光一个模型是不够的，需要多个模型一起运作，这时我们需要一个类，来对这些模型进行统一操作。

例如在GBDT中，我们定义了多个PSModel，并添加到MLModel中

```
  addPSModel(SKETCH_MAT, sketch)
  addPSModel(FEAT_SAMPLE_MAT, featSample)
  addPSModel(SPLIT_FEAT_MAT, splitFeat)
  …………

```

整体性的操作，包括predict，setSavePath……都统一通过MLModel进行。但是算法对远程模型的具体操作，都是通过PSModel来进行
   
## **MLRunner（启动器）**

所有的算法都需要一个启动类，在Angel中，启动类需要继承MLRunner，并实现train和predict两个方法

最简单的Runner，使用标准流程的话，非常的简单：

* train方法

```
 train(conf: Configuration)
```

* predict方法
	
```
predict(predict(conf: Configuration))
```

## **TrainTask & PredictTask（任务类）**

很遗憾，在最后，我们还需要编写两个任务类，有点类似`Hadoop`的`Mapper`和`Reducer`，当然了，它们本质上是完全不同的，但是作用类似，都是将任务封装好，传给远程的Worker去分布式启动和执行。

Angel的这两个Task中，都是BaseTask的子类，Angel会负责透明的数据切分和读取，并Feed给这些Task类，用户需要定义的，其实主要是2个公共操作：

2. **解析数据（parse）**
	* 将单行数据解析为算法所需的数据结构（例如用于LR/SVM等算法的LabeledData）
	* 没有默认实现，必须实现

3. **预处理（preProcess）**
	* 读取原始数据集，按行调用parse方法，将数据进行简单预处理，需要的话，切换训练集（Training Set）和 检验集（Validation Set）
	* 有默认实现，可以不实现

当然了，TrainTask需要实现train方法，PredictTask需要实现predict方法，这些都有默认的实现，具体Task需要做的，其实只是把MLModel创建好，传递给父类方法就好。


## Spark on Angel

Angel本身的算法编写，接口略微简朴。但是这种方式，可以让Angel更加Focus在并提升机器学习算法性能，而无需过多关注其它事情。

但是有些用户希望更加灵活的写法，或者他们希望Spark也能基于Angel获得高维度模型的训练能力。为此在Spark on Angel上，也可以有体现和支持。具体可以查看：[《Spark on Angel 编程指南》]()




