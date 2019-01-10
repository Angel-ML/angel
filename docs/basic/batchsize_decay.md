# Angel 中的学习率Decay

Angel参考TensorFlow实现了多种学习率Decay方案, 用户可以根据需要选用. 在描述具体Decay方案前, 先了解一下Angel中的Decay是怎样引入的, 在什么时候进行Decay.

对于第一个问题, Decay是在`GraphLearner`类中引入的, 在初始化时有如下代码:
```scala
val ssScheduler: StepSizeScheduler = StepSizeScheduler(SharedConf.getStepSizeScheduler, lr0)
```
其中`StepSizeScheduler`是所有Decay的基类, 同名的object是所有Decay的工场, `SharedConf.getStepSizeScheduler`通过读取"ml.opt.decay.class.name"的值可获是指定的decay类型(默认是StandardDecay).

第二个问题, Angel提供了两种方案:
- 一个mini-batch一次Decay
- 一个epoch一次Decay

通过"ml.opt.decay.on.batch"参数进行控制, 当其为true时, 一个mini-batch一次Decay, 当其为flase(默认的方式)一个epoch一次Decay. 具体代码在`GraphLearner`类中的train方法与trainOneEpoch方法中.


## 1. ConstantLearningRate
这是最简单的Decay方式, 就是不Decay, 学习率在整个训练过程中不变

配置样例:
```
ml.opt.decay.class.name=ConstantLearningRate
```

## 2. StandardDecay
标准Decay方案, 公式如下:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}})

配置样例:
```
ml.opt.decay.class.name=StandardDecay
ml.opt.decay.alpha=0.001
```
## 3. CorrectionDecay
修正Decay, 这种方案适合于Momentum, 它是专为Momentum设计的, 请不要用于Adam等其它优化器. 计算公式为:

![](http://latex.codecogs.com/png.latex?lr_{t}=\frac{lr_0}{\sqrt{1+\alpha\cdot%20t}}\cdot\frac{1-\beta}{1-\beta^t})

第一部分就是StandardDecay, 它是正常的Decay, 延续二部分是修正项, 为Momentum设计, 它是运动量系数之和的倒数. 其中$\beta$必须与优化器中的momentum相等. 一般可设为0.9. 

这种Decay的使用有两个注意点:
- 动量计算公式应为: velocity = momentum * velocity + gradient, Angel中Momentum的实南已是这种方式
- 要求一个mini-batch一次Decay, 因为要与参数的update同步

配置样例:
```
ml.opt.decay.class.name=CorrectionDecay
ml.opt.decay.alpha=0.001
ml.opt.decay.beta=0.9
```
## 4. WarmRestarts
这是一种较为高级的Decay方案, 它是周期中Decay的代表. 标准计算公式如下:

![](http://latex.codecogs.com/png.latex?lr_t=lr_{min}+\frac{1}{2}\cdot\frac{lr_{max}-lr_{min}}{1+\cos{(\frac{t}{interval}\pi)}})

对于标准计算公式, 我们做了如下改进. 
- 对![](http://latex.codecogs.com/png.latex?lr_{max})进行衰减
- 遂步增大![](http://latex.codecogs.com/png.latex?interval)

配置样例:
```
ml.opt.decay.class.name=WarmRestarts
ml.opt.decay.alpha=0.001
```

其中![](http://latex.codecogs.com/png.latex?interval)通过`ml.opt.decay.intervals`设置, 具体如下:
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
