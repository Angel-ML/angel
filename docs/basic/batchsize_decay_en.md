# Learning Rate Decay in Angel

Angel refers to TensorFlow to implement a variety of learning rate Decay schemes. Users can choose Decay schemes according to their needs. Before describing the specific Decay schemes, first learn how Decay in Angel was introduced and when Decay was implemented.

For the first problem, Decay was introduced in the `GraphLearner'class, with the following code for initialization:
```scala
val ssScheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, lr0)
```
Where `StepSize Scheduler'is the base class of all Decays, object with the same name is the workshop of all Decays, and `SharedConf. getStepSize Scheduler' can obtain the specified decay type by reading the value of `ml. opt. decay. class. name'(default is StandardDecay).
For the second question, Angel offers two options:
- A mini-batch once Decay
- A epoch once Decay

Through the parameter of "ml.opt.decay.on.batch", when it is true, a mini-batch once Decay, and an epoch once Decay when it is flase (default way). Specific code in the `GraphLearner'class of train method and trainOneEpoch method.

## 1. ConstantLearningRate
This is the simplest way to Decay, that is, without Decay, the learning rate remains unchanged throughout the training process.

Configuration sample:
```
ml.opt.decay.class.name=ConstantLearningRate
```

## 2. StandardDecay
The standard Decay scheme, the formula is as follows:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}})

Configuration sample:
```
ml.opt.decay.class.name=StandardDecay
ml.opt.decay.alpha=0.001
```
## 3. CorrectionDecay
Modified Decay, this scheme is suitable for Momentum, it is designed for Momentum, please do not use it for other optimizers such as Adam. The calculation formula is:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}}\cdot\frac{1-\beta}{1-\beta^t})

The first part is StandardDecay, which is the normal Decay, and the second part is the correction item, designed for Momentum, which is the reciprocal of the sum of the motion coefficients. Where $\beta$ must be equal to the momentum in the optimizer. Generally set to 0.9 .

There are two points to note about the use of this Decay:
- The momentum calculation formula should be: velocity = momentum * velocity + gradient, the implementation of Momentum in Angel is also this way.
- Requires a mini-batch once Decay, because it is synchronized with the update of the parameter.

Configuration sample:
```
ml.opt.decay.class.name=CorrectionDecay
ml.opt.decay.alpha=0.001
ml.opt.decay.beta=0.9
```
## 4. WarmRestarts
This is a more advanced Decay scheme, which is representative of Decay in the cycle. The standard calculation formula is as follows:
![](http://latex.codecogs.com/png.latex?lr_t=lr_{min}+\frac{1}{2}\cdot\frac{lr_{max}-lr_{min}}{1+\cos{(\frac{t}{interval}\pi)}})

For the standard calculation formula, we made the following improvements.
- Attenuate ![](http://latex.codecogs.com/png.latex?lr_{max})
- Step by step to increase ![](http://latex.codecogs.com/png.latex?interval)

Configuration sample:
```
ml.opt.decay.class.name=WarmRestarts
ml.opt.decay.alpha=0.001
```

Where![](http://latex.codecogs.com/png.latex?interval) is set by `ml.opt.decay.intervals`, as follows:
```scala
class WarmRestarts(var etaMax: Double, etaMin: Double, alpha: Double) extends StepSizeScheduler {

  var current: Double = 0
  var numRestart: Int = 0
  var interval: Int = SharedConf.get().getInt(MLConf.ML_OPT_DECAY_INTERVALS, 100)

  override def next(): Double = {
    current += 1
    val value = etaMin + 0.5 * (etaMax - etaMin) * (1 + math.cos(current / interval * math.Pi))
    if (current == interval) {
      current = 0
      interval *= 2
      numRestart += 1
      etaMax = etaMax / math.sqrt(1.0 + numRestart * alpha)
    }

    value
  }

  override def isIntervalBoundary: Boolean = {
    current == 0
  }

}
```
