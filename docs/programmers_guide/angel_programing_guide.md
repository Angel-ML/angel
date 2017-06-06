# Angel编程指南（Angel Programing Guide）

---

Angel的设计理念，是针对`机器学习和模型`而来。V1版本的接口，会崇尚简约，利用PS的本质来实现可变的模型，略为生硬。

在后续的版本，会引入更多的灵活性，函数式编程，以及深度学习友好的特性。

> Angel在SQL和数据处理方面，现在不会，将来也不会，做太多的开发，Spark等在这方面已经做得足够好了，请先用合适工具进行特征预处理，核心的算法，再交由Angel来做。


## **MLModel** 和 **PSModel**

在Angel中，`模型`是一等公民，实现一个定制的新算法，一定要先考虑好模型要怎么样设计。


**PSModel（分布式模型）**

* PSModel是Angel的核心抽象。它是一个远程模型的概念，对于Client来说，它是一个类似`模型代理`的类。通过它，你可以在Worker上，像操作本地对象一样的，去操作一个模型，而实际上，你在操作的是，一个均匀切分在远程多个PSServer上的**分布式模型**，而且所有的操作都是透明并发的。

* 所以，实现一个算法的最小集，也要定义一个PSModel，例如：

```
  val loss = PSModel[DenseDoubleVector](ctx, 
  								      "lr_loss_martrix", 
  								       1, 
  								       epochNum)
```


**MLModel（模型集）**

* 很多复杂的算法，一个模型是不够的，需要多个模型一起运作，这时我们需要一个类，来对这些模型进行统一操作

* 例如在GBDT中，我们定义了多个PSModel，并添加到MLModel中

```
  addPSModel(SKETCH_MAT, sketch)
  addPSModel(FEAT_SAMPLE_MAT, featSample)
  addPSModel(SPLIT_FEAT_MAT, splitFeat)
  …………

```

* 整体性的操作，包括：
	* predict
	* setSavePath
	* setLoadPath

   都统一通过MLModel进行。但是算法对模型的具体操作，都是通过PSModel来进行
   
**MLRunner（启动器）**

所有的算法都需要一个启动类，在Angel中，启动类需要继承MLRunner，并实现Train和Predict两个方法

最简单的Runner，使用标准流程的话，非常的简单：

* train方法

```
 train(conf, LRModel(conf), classOf[LRTrainTask])
```

* predict方法
	
```
predict(conf, LRModel(conf), classOf[LRPredictTask])
```

**TrainTask & PredictTask（任务类）**

很遗憾，在最后，我们还需要编写两个任务类，有点类似`Hadoop`的`Mapper`和`Reducer`，当然了，实际上是实现是完全不同的功能，但是作用类似，都是将任务封装好，传给远程的Worker去分布式启动和执行。

Angel的这两个Task中，都是BaseTask的子类，Angel会负责透明的数据切分和读取，并Feed给这些Task类，用户需要定义的，其实主要是2个公共操作：

2. **解析数据（Parse）**
	* 将单行数据解析为LabeledData
	* 没有默认实现，必须实现

3. **预处理（preProcess）**
	* 调用Parse方法，将数据进行简单预处理，需要的话，切换训练集（Training Set）和 检验集（Validation Set）
	* 有默认实现，可以不实现

当然了，TrainTask需要实现train方法，PredictTask需要实现Predict方法，这些都有默认的实现，具体Task需要做的，其实只是把MLModel创建好，传递给父类方法就好。

目前来看，这种编写算法的方式，略为笨拙。但是这种简单的方式，可以让Angel更加Focus在PS相关的本身计算，并提升性能，而无需过多关注其它事情。

而灵活的写法，在Spark on Angel上，也可以有体现和支持。




