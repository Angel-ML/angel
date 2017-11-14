# Core API Classes

---

![](../img/angel_class_diagram.png)

As shown above, Angel's core API classes, ordered by when (in general) they are called during model training, include:

1. [MLRunner](MLRunner_en.md)
	* MLRunner creates AngelClient with factory class based on conf, and calls AngelClient's interfaces in order according to the standard `train` process

* [AngelClient](AngelClient_en.md)
	* Starts PSServer
	* Initializes PSServer and loads empty model
	* After training, saves the model to HDFS from multiple PSServers

* [TrainTask](Task_en.md)
	* Starts `train` process when called by AngelClient

* [DataBlock](DataBlock_en.md)
	* TrainTask calls `parse` and `preProcess` methods to read data from HDFS, and assemble data into DataBlock that contains multiple LabeledData
	* TrainTask calls `train` method to create, and pass DataBlock to, the MLLearner object

* [MLLearner](MLLearner_en.md)
	* MLLearner calls its own `learn` method, reads DataBlock, computes the model delta, and pushes to / pull from PSServer through PSModel inside MLModel, eventually obtaining a complete MLModel

* [MLModel](MLModel_en.md)
	* According to the algorithm's need, creates and holds multiple PSModels

* [PSModel](PSModel_en.md)
	* Encapsulates all the interfaces in AngelClient that communicate with PSServer, facilitating MLLearner calls

 Understanding these core classes and processes will be quite helpful for implementing high performance machine-learning algorithms that can run on Angel.
