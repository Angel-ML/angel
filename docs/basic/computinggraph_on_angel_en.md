# Computational Graphs in Angel

## 1. What are Computational Graphs?
Computational graphs are commonly used in mainstream deep learning frameworks such as TensorFlow, Caffe and MXNet. In addition, many big data frameworks like Spark  also adopt computational graphs to implement task scheduling. Computational graph framework is also available on Angel, but rather more lightweight than on TensorFlow etc., mainly in terms of:

- **Coarse Granularity**: a node in Angel's computational graphs represents a `layer` instead of an `operator`. TensorFlow's design strategy that uses `operator` as a node allows highly flexible development, and is very suitable for second development (encapsulation). Yet it also brought a steeper learning curve and larger workload to algorithm developers, hence resulted in criticism on the old-version TensorFlow about "excessively low-level APIs and low development efficiency" until the later version provided layer-based high-level APIs. Given this, Angel only provides coarse-grained computational graphs that are based on layers.
- **Feature Intersection**: for recommendation system-related machine learning algorithms, features after Embedding will often undergo some intersection before being input to DNN. (**Note**: different from the artificial intersection in general feature engineering, feature intersection here refers to the specific operations on Embedding's output.) Implementations of feature intersection on TensorFlow, Caffe and Torch are relatively cumbersome , but Angel has directly offered this feature intersection layer.
- **Auto-Generated Network**: Drawing from Caffe's idea, Angel also supports generating deep neural networks from JSON files, which grately reduces users' workload as they can generate their own neural networks without writing any code.

Note that Angel does not currently support CNN, RNN, etc., but only focuses on deep learning algorithms commonly used in recommendations.

## 2. Construction of Computational Graphs

### 2.1 Basic Structure of a Layer
It will be helpful understanding the structure of its composing unit, the layers, before learning how a computational graph is built. (For more detailed information about layers, please refer to [Layers on Angel](./layers_on_angel.md).)

```scala
abstract class Layer(val name: String, val outputDim: Int)(implicit val graph: AngelGraph)
  extends Serializable {
  var status: STATUS.Value = STATUS.Null
  val input = new ListBuffer[Layer]()
  val consumer = new ListBuffer[Layer]()

  def addInput(layer: Layer): Unit = {
    input.append(layer)
  }

  def addConsumer(layer: Layer): Unit = {
    consumer.append(layer)
  }

  def calOutput(): Matrix = ???

  def gatherGrad(): Matrix = ???
}
```
The abstract class above has well demonstrated the major functions of a layer:
- **status**: nodes in Angel's computational graphs are stateful. A node's status is handled by a state machine, as described in the next section
- **input**: a `ListBuffer` object that is used to record this node/layer has some input. One layer can hold multiple input layers, which can be sequentially added via `addInput(layer: Layer)`.
- **outputDim**: there can be at most one output in Angel. `outputDim` is used to specify the dimension of the output
- **consumer**: though there's only one output from each layer, the output node can be consumed by multiple consumers, represented by a `ListBuffer`. When building the graph, users may call the input layer's `addConsumer(layer: Layer)` method to specify by which layers the output layer will be consumed. 

In fact, users do not need to care about the specific operations of building a graph, as they have already been completed in base classes such as `InputLayer`, `LinearLayer` and `JoinLayer`:

```scala
abstract class InputLayer(name: String, outputDim: Int)(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  graph.addInput(this)

  def calBackward(): Matrix
}

abstract class JoinLayer(name: String, outputDim: Int, val inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  inputLayers.foreach { layer =>
    layer.addConsumer(this)
    this.addInput(layer)
  }

  def calGradOutput(idx: Int): Matrix
}

abstract class LinearLayer(name: String, outputDim: Int, val inputLayer: Layer)(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  inputLayer.addConsumer(this)
  this.addInput(inputLayer)

  def calGradOutput(): Matrix
}
```
Note: LossLayer is a special LinearLayer, hence it's not shown here.

### 2.2 Basic Structure of AngelGraph
Now we have constructed a complicated graph using input/consumer. Though the graph can be traversed from any node in it, AngelGraph still stores the verge node, so as to facilitate the operations on graph:

```scala
class AngelGraph(val placeHolder: PlaceHolder, val conf: SharedConf) extends Serializable {

  def this(placeHolder: PlaceHolder) = this(placeHolder, SharedConf.get())

  private val inputLayers = new ListBuffer[InputLayer]()
  private var lossLayer: LossLayer = _
  private val trainableLayer = new ListBuffer[Trainable]()

  
  def addInput(layer: InputLayer): Unit = {
    inputLayers.append(layer)
  }

  def setOutput(layer: LossLayer): Unit = {
    lossLayer = layer
  }

  def getOutputLayer: LossLayer = {
    lossLayer
  }

  def addTrainable(layer: Trainable): Unit = {
    trainableLayer.append(layer)
  }

  def getTrainable: ListBuffer[Trainable] = {
    trainableLayer
 }
```
There are two major types of verges:
- inputLayer: data is the input of this type of nodes. Storing inputLayers in AngelGraph facilitates back-propagation, which now can be done by calling the  `calBackward` method in each inputLayer sequentially. In order to get inputLayers fully included, Angel requires each inputLayer add itself explicitly to the AngelGraph by calling the `addInput` method of AngelGraph. In fact, users don't neet to care about implementing these operations since they have already been included in the  `InputLayer` base class.
- lossLayer: Angel doesn't currently support multi-task learning, thus each computational graph can have only one lossLayer. The lossLayer node is designed for the convenience of forward propagation, which can here be easily done by calling the `predict` or `calOutput` method of the lossLayer node. As `LossLayer` is a subclass of `LinearLayer`, user-defined lossLayer can be added by manually calling `setOutput(layer: LossLayer)`. However, it is more common to add a lossFunc instead of lossLayer.

With inputLayers and lossLayer included, it is very easy to traverse the AngelGraph by forward propagation using the `predict` method in lossLayer, and by back propagation using `calBakcward` method in inputLayer. The last problem is to update the parameters in gradient computation. Fortunately, AngelGraph makes the parameter update convenient by holding a `trainableLayer` variable, which collects all layers that have parameters to be updated.




## 2.3 Entrance of Data: PlaceHolder
Now we have constructed the edges (relations among nodes) by clarifying the input/consumer of layers, and stored special nodes (inputLayer/lossLayer/trainableLayer) for forward/back propagation and parameter update. So how does the data get input? The answer is to use a `PlaceHolder`.

When construcing AngelGraph, the PlaceHolder is passed down to Graph, which will be then passed down to all layers as an implicit parameter. In this way, every layer has an access to the PlaceHolder, namely, the input data.

Angel currenly allows only one PlaceHolder in each graph, but this limitation will be removed in future version to support multiple data input. PlaceHolder only holds a mini-batch of data, as demonstrated below:

```scala
class PlaceHolder(val conf: SharedConf) extends Serializable {
    def feedData(data: Array[LabeledData]): Unit
    def getFeats: Matrix
    def getLabel: Matrix
    def getBatchSize: Int
    def getFeatDim: Long
    def getIndices: Vector
}
```
After inputting data in  `Array[LabeledData]` format to PlaceHolder via `feedData`, we can get following metrics from the PlaceHolder:

- Feature
- Feature dimension
- Label
- BatchSize
- Feature index

## 3. Operating Principle of Computational Graph in Angel
We have constructed the topological structure of a computational graph in the prvious section. In this section, we are going to learn how the graph works in Angel.

### 3.1 State Machine
A state machine in Angel has the following states:

- Null: The initial state. The Graph will be set to this state after each `feedData` process
- Forward: This state indicates the completion of forward propagation
- Backward: This state indicates that the back propagation has been finished, but the gradients of parameters has not been calculated yet
- Gradient: This state indicates that the gradients have been calculated and pushed to the PS
- Update: This state indicates that the model update is complete

These states are in turn, as shown in the figure below.

![状态机](../img/status.png)

The state machine is basically designed for ensuring the order of operations, thus reducing the re-calculations. For instance, let's say there are multiple layers going to consume the output of the same layer. In calculation, the program will check the current status of the layer before truly executing the operation, so the calculation only needs to be calculated once. The representation of state machine in code is demonstrated as follows.

```scala
def feedData(data: Array[LabeledData]): Unit = {
    deepFirstDown(lossLayer.asInstanceOf[Layer])(
        (lay: Layer) => lay.status != STATUS.Null,
        (lay: Layer) => lay.status = STATUS.Null
    )

    placeHolder.feedData(data)
}

override def calOutput(): Matrix = {
    status match {
        case STATUS.Null =>
        // do come forward calculation
        status = STATUS.Forward
        case _ =>
    }
    output
}


override def calBackward(): Matrix = {
    status match {
        case STATUS.Forward =>
        val gradTemp = gatherGrad()
        // do backward calculation
        status = STATUS.Backward
        case _ =>
    }

    backward
}

override def pushGradient(): Unit = {
    status match {
        case STATUS.Backward =>
        // calculate gradient and push to PS
        status = STATUS.Gradient
        case _ =>
    }
}

override def update(epoch: Int = 0): Unit = {
    status match {
        case STATUS.Gradient =>
        optimizer.update(weightId, 1, epoch)
        status = STATUS.Update
        case _ => 
        throw new AngelException("STATUS Error, please calculate Gradient frist!")
    }
}
```

### 3.2 Training Process of Graph in Angel
Here we just construct a overall frame of the training process. The detailed codes are provided in`GraphLearner`.
```scala
def trainOneEpoch(epoch: Int, iter: Iterator[Array[LabeledData]], numBatch: Int): Double = {
    var batchCount: Int = 0
    var loss: Double = 0.0
    while (iter.hasNext) {
      graph.feedData(iter.next())
      graph.pullParams()
      loss = graph.calLoss() // forward
      graph.calBackward() // backward
      graph.pushGradient() // pushgrad
      PSAgentContext.get().barrier(ctx.getTaskId.getIndex)
      if (ctx.getTaskId.getIndex == 0) {
        graph.update(epoch * numBatch + batchCount) // update parameters on PS
      }
      PSAgentContext.get().barrier(ctx.getTaskId.getIndex)
      batchCount += 1

      LOG.info(s"epoch $epoch batch $batchCount is finished!")
    }

    loss
  }
```
The process works as follows:
- feedData: This process will set the Graph's state to `null`
- Pull parameters: Only pulls those parameters required in calculation of the current mini-batch, basing on the input data. In this way, Angel is capable of training very high-dimensional models.
- Forward calculation: Beginning from the LossLayer, this process will call the `calOutput` method of each layer's inputLayer in cascade, calculate the output in sequence, then set the layer's state to `forward`. For those layers already at `forward` state, Angel will directly return the result from the previous calculation, avoiding re-calculation.
- Backward calculation: This process sequentially invokes the inputLayer of the Graph, thus calls the `calGradOutput` method of the first layer in cascade to finish the back propagation. When the calculation is completed, the layer's state will be set to `backward`. Similarly, for those layers already at `backward` state, Angel will directly return the result from the previous calculation, avoiding re-calculation.
- Calculation and update of gradients: the previous step only calculates the gradients of nodes, while the gradients of parameters are calculated in this step simply via the `pushGradient` method of `trainable`. This method will firstly compute the gradients, then push them to PS, and finally set the state to `gradient`.
- Gradient update: since gradients are updated on PS, it is enough sending only one PSF of gradient update by one Worker (by Driver in Spark on Angel). The update methods differ in different optimizers, while the optimizer in Angel is basically a PSF. A sync is needed before updating the parameters to ensure that all gradients are already pushed; And there should be another sync after updating the parameters to ensure that all parameters pulled by Worker are up-to-date. The state will be set to `update` after the update is finished.
