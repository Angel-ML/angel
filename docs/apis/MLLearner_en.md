# MLLearner

> MLLearner is a core class for model training.  In principle, any core model-training logic in Angel should be implemented and called by this class. MLLearner is `train`'s core class. 

## Function

* Reading DataBlock constantly, training to obtain the MLModel

## Core Interface

1. **train**
	- Definition: ```def train(train: DataBlock[LabeledData], vali: DataBlock[LabeledData])：MLModel```
	- Function: train the model on the training data using a specific algorithm
	- Parameters: train: DataBlock[LabeledData] training dataset;  vali: DataBlock[LabeledData] validation dataset
	- Return value: MLModel
