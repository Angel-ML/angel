# Leraning Rate Decay in Angel

Referring to TensorFlow, Angel implements a variety of learning rate decay solutions, from which users can select at will. Before depicting the Decay solutions in detail, let's take a first look on how Decay is introduced in Angel and when we should use Decay.

Decay is introduced to Graph in the `GraphLearner` class. Following codes are executed during initialization:

```scala
val ssScheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, lr0)
```
The `StepSizeScheduler` is the base class of all Decay, and the object of the same name is the factory of all Decay instances. `SharedConf.getStepSizeScheduler` can obtain the specified decay type by reading the value of **ml.opt.decay.class.name** (StandardDecay by default).

For the second question, Angel provides two solutions controlled by the **ml.opt.decay.on.batch** parameter:

- One Decay per mini-batch (true)
- One Decay per epoch (false; default)

Detailed codes are included in the `train` and `trainOneEpoch` methods of the  `GraphLearner` class.


## 1. ConstantLearningRate
ConstantLearningRate is the simplest Decay, that is, the learning rate keeps unchanged throughout the training process without any decay.

Configuration example:
```
ml.opt.decay.class.name=ConstantLearningRate
```

## 2. StandardDecay
The standard Decay solution. The formula is:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}})

Configuration example:
```
ml.opt.decay.class.name=StandardDecay
ml.opt.decay.alpha=0.001
```
## 3. CorrectionDecay
The Correction Decay is explicitly designed for Momentum. Please do not use it for other optimizers such as Adam. The formula is:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}}\cdot\frac{1-\beta}{1-\beta^t})

The first part of the multiplication is exactly the StandardDecay; The second part is the correction designed for Momentum, which is the reciprocal of the sum of the motion coefficients. The $\beta$ must be equal to the momentum in optimizer, and can be generally set to 0.9.

There are two things to note about using CorrectionDecay:
- The formula of calculating the momentum should be $velocity = momentum * velocity + gradient$. It is already included in the implementation of Momentum in Angel.
- As it requires one Decay per mini-batch, the Decay should be synchronous with parameter update.

Configuration example:
```
ml.opt.decay.class.name=CorrectionDecay
ml.opt.decay.alpha=0.001
ml.opt.decay.beta=0.9
```
## 4. WarmRestarts
WarmRestarts is a more advanced Decay solution, which is representative of Decay in cycle. The standard calculation formula is as follows:

![](http://latex.codecogs.com/png.latex?lr_t=lr_{min}+\frac{1}{2}\cdot\frac{lr_{max}-lr_{min}}{1+\cos{(\frac{t}{interval}\pi)}})

We make following improvements for the standard calculation formula:

- Attenuate the ![](http://latex.codecogs.com/png.latex?lr_{max})
- Successively increase the ![](http://latex.codecogs.com/png.latex?interval)

Configuration example:
```
ml.opt.decay.class.name=WarmRestarts
ml.opt.decay.alpha=0.001
```

The ![](http://latex.codecogs.com/png.latex?interval) is configured via `ml.opt.decay.intervals`:
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
