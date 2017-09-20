# Angel Programming Guide

---

Angel's design philosophy is `machine-learning model oriented`. Simpleness is the principle for the overall interface design，taking advantage of the essence of the Parameter Server (PS) to support high-dimensional models. Angel's primary abstraction, the `PSModel`, transparentize the remote model operations distributed across multiple PS Servers. With PSModel，algorithm developer can easily **update models，define customized functions, and control synchronization** to realize a variety of efficient machine learning algorithms.


## **PSModel** and **MLModel**  

In Angel, `models` are first-class citizens. Designing and implementing an algorithm in Angel is straightforward via the model-based approach.

It is worth mentioning that PSModel is `mutable`, and is thread-safe and transparent   distributed to users. This is the major distinction from and a big advantage over Spark (immutable RDD)。


**PSModel**

PSModel is Angel's core abstraction. A remote model concept, it works similarly as a model-proxy class for the client. One can operate a model on each worker as if operating a local object; however, what is actually being operated is a slice of the model, where the model is evenly distributed on multiple remote PSServers, and all the operations are transparent and concurrent.

A simplest Angel algorithm contains a PSModel，for example：

```
  val loss = PSModel[DenseDoubleVector]("lr_loss_martrix", 1, epochNum)
```


**MLModel**

Sometimes, a complex algorithm involves multiple models. In this case，we need to package these models into one class, so that we can better perform overall operation on them.  

Taking GBDT as an example，we define multiple PSModels to add in the MLModel

```
  addPSModel(SKETCH_MAT, sketch)
  addPSModel(FEAT_SAMPLE_MAT, featSample)
  addPSModel(SPLIT_FEAT_MAT, splitFeat)
  …………

```

Overall operations，including predict，setSavePath…… are done in MLModel; however, the specific operations on the remote model is done in PSModel.
   
## **MLRunner**

All algorithms need a startup class. In Angle, a startup class inherits MLRunner and realizes the train method and predict method. 

Let's look at the simplest Runner under the standard procedure

* train 

```
 train(conf: Configuration)
```

* predict 
	
```
predict(predict(conf: Configuration))
```

## **TrainTask & PredictTask**

Finally, we need to define two task classes that are similar to `Hadoop`'s `Mapper` and `Reducer`, though they are different in essence. The goal is to encapsulate the tasks and send to remote workers for execution.

In Angel, these two task classes are both subclasses of the BaseTask. Angel partitions and reads data transparently, and feeds them to the task classes. Essentially, users just need to define two public methods shown below：

2. **parse**
	* parses a single-line's data into the data structure required by the algorithm（for example, LabeledData for LR/SVM）
	* there is no default implementation, so it must be inherited.

3. **preProcess**
	* reads the raw data，calls parse for each line, and preprocesses data. If necessary,  spliting data into training set and validation set will happen.
	* there is a default implementation, it can be simply 

Of course, TrainTask needs the train method and PredictTask needs the predict method，and both have default implementations. A specific task just needs to create the MLModel and pass it to its parent class.


## Spark on Angel

The algorithm implementation and interface in Angel is a bit simple, but it is the reason why Angel can focus on improving the performance of machine learning algorithms, rather than other things. 

That said, we understood that some users may want more flexible implementations or training high-dimensional model in Spark on the Angel platform. Spark on Angel reflects these needs and provides corresponding support.  Please read the [Spark on Angel Programming Guide](spark_on_angel_programing_guide_en.md) for more details. 




