# Core API Classes

---

![](../img/angel_class_diagram.png)

As shown above, Angel's core API classes, ordered by when they are called during model training in general, include: 

1. [MLRunner](MLRunner.md)
	* MLRunner creates AngelClient from the factory based on Conf, and calls AngelClient's interfaces in order according to the standard `train` process

* [AngelClient](AngelClient.md)
	* Starts PSServer
	* Initializes PSServer and load empty model
	* After training, saves the model to HDFS from multiple PSServers

* [TrainTask](Task.md)
	* Starts `train` when called by AngelClient

* [DataBlock](DataBlock.md)
	* TrainTask calls `parse` and `preProcess` methods to read data from HDFS and assemble the data into DataBlock that contains multiple LabeledData 
	* TrainTask calls `train` method to create, and pass DataBlock to, the MLLearner object 
	
* [MLLearner](MLLearner.md)
	* MLLearner calls its own `learn` method, reads DataBlock, computes the model delta, and pushes to / pull from with PSServer through the PSModel within the MLModel, eventually obtaining a complete MLModel

* [MLModel](MLModel.md)
	* According to the algorithm's need, creates and holds multiple PSModels

* [PSModel](PSModel.md)
	* Encapsulates all the interfaces in AngelClient that communicate with PSServer, facilitating MLLearner calls



It will be helpful to understand these core classes and processes for implementing machine-learning algorithms that can achieve good performance on Angel. 

