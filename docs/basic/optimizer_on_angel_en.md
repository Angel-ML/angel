# Optimizers in Angel

There are various optimization methods in machine learning, but in the big data use cases, the most commonly used method is a series of optimizations based on SGD. Currently, only a small number of optimization methods are implemented in Angel:

- Stochastic gradient descent-based methods:
    - SDG: here referring to mini-batch SGD 
    - Momentum: SGD with momentum
    - AdaGrad: SGD with diagonal Hessian approximation
    - AdaDelta: an extension of AdaGrad adapting learning rates based on a moving window of gradient updates
    - Adam: SGD with momentum and diagonal Hessian approximation
- Online learning methods:
    - FTRL: Follow The Regularized Leader




## 1. SGD
The update rule of SGD is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}\bold{x}_{t+1}=\bold{x}_t-\eta\Delta\bold{x}_t)

in which ![](http://latex.codecogs.com/png.latex?\eta) represents the learning rate. SGD supports including ![](http://latex.codecogs.com/png.latex?L_1,L_2) regularization, and the exact optimization used in this case is PGD (proximal gradient descent). 

There are two ways to represent SGD optimization in Json:

```json
"optimizer": "sgd",

"optimizer": {
    "type": "sgd",
    "reg1": 0.01,
    "reg2": 0.02
}
```

## 2. Momentum
The update rule of Momentum is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}\bold{v}_t=\gamma\bold{v}_{t-1}+\Delta\bold{x}_t,\bold{x}_{t+1}=\bold{x}_t-\eta\bold{v}_t)

in which ![](http://latex.codecogs.com/png.latex?\gamma) represents the momentum factor, ![](http://latex.codecogs.com/png.latex?\eta) represents the learning rate. Momentum supports![](http://latex.codecogs.com/png.latex?L_2) regularization. Momentum is the default optimization method in Angel.

There are two ways to represent Momentum in Json:
```json
"optimizer": "momentum",

"optimizer": {
    "type": "momentum",
    "momentum": 0.9,
    "reg2": 0.01
}
```

## 3. AdaGrad
The update rule of AdaGrad (referring to the exponential smoothing version, i.e., RMSprop) is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}n_t=\beta%20n_{t-1}+(1-\beta)g_{t}^2)

![model](http://latex.codecogs.com/png.latex?\dpi{150}\Delta%20\theta=-\frac{\eta}{\sqrt{n_t+\epsilon}}\cdot%20g_t)

in which ![](http://latex.codecogs.com/png.latex?\beta) is the smoothing factor and ![](http://latex.codecogs.com/png.latex?\eta) is the learning rate. AdaGrad also supports![](http://latex.codecogs.com/png.latex?L_1,L2) regularization.

There are two ways to represent AdaGrad in Json:
```json
"optimizer": "adagrad",

"optimizer": {
    "type": "adagrad",
    "beta": 0.9,
    "reg1": 0.01,
    "reg2": 0.01
}
```

## 4. AdaDelta
The update rule of AdaDelta is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}n_t=\alpha%20n_{t-1}+(1-\alpha)g_{t}^2)

![model](http://latex.codecogs.com/png.latex?\dpi{150}\Delta%20\theta=-\frac{\sqrt{v_{t-1}+\epsilon}}{\sqrt{n_t+\epsilon}}\cdot%20g_t)

![model](http://latex.codecogs.com/png.latex?\dpi{150}v_t=\beta%20v_{t-1}+(1-\beta)\Delta%20\theta_{t}^2)


in which ![](http://latex.codecogs.com/png.latex?\alpha,%20\beta) are smoothing factors. AdaDelta also supports![](http://latex.codecogs.com/png.latex?L_1,L2) regularization.

There are two ways to represent AdaDelta in Json:
```json
"optimizer": "adadelta",

"optimizer": {
    "type": "adadelta",
    "alpha": 0.9,
    "beta": 0.9,
    "reg1": 0.01,
    "reg2": 0.01
}
```

## 5. Adam
Adam is a better way to optimize. The formula is:

![model](http://latex.codecogs.com/png.latex?\dpi{150}\begin{array}{ll}\bold{m}_t&=\beta\bold{m}_{t-1}+(1-\beta)\Delta\bold{x}_t\\\\\\bold{v}_t&=\gamma\bold{v}_{t-1}+(1-\gamma)\Delta\bold{x}^2_t\\\\\bold{x}_t&=\gamma\bold{x}_{t-1}-\eta\frac{\sqrt{1-\gamma^t}}{1-\beta^t}\frac{\bold{m}_t}{\sqrt{\bold{v}_t}+\epsilon}\\%20\end{array})

in which ![](http://latex.codecogs.com/png.latex?\bold{m}_t) represents the exponential smoothing factor, i.e. the momentum, and  ![](http://latex.codecogs.com/png.latex?\bold{v}_t) represents the exponential smoothing of gradient ![](http://latex.codecogs.com/png.latex?\bold{x}^2_t), which can be regarded as diagonal approximation of Hessian. ![](http://latex.codecogs.com/png.latex?\beta=0.9,\gamma=0.99) by default. 

Let

![model](http://latex.codecogs.com/png.latex?\dpi{150}f(t)=\frac{\sqrt{1-\gamma^t}}{1-\beta^t})

then ![](http://latex.codecogs.com/png.latex?f(t)) is a function with initial value of 1 and a limit of 1, which decreases first and then increases:
![adam系数](../img/adam_coeff.png)

Which means the learning rate is decreased during the initial state of optimization where the gradient is relatively large, so as to smooth the gradient descent; and during the final state where the gradient is very small, the learning rate will be approximately increased in order to help jumping out of local minimum.

There are two ways to represent Adam in Json:
```json
"optimizer": "adam",

"optimizer": {
    "type": "adam",
    "beta": 0.9,
    "gamma": 0.99,
    "reg2": 0.01
}
```

## 4. FTRL
FTRL is an online learning algorithm which goal is to optimize the regret bound. It is proved to be effective under specific learning rate decay condition.

Another characteristic of FTRL that distinguishes it from PGD (proximal gradient descent) and other online learning methods such as FOBOS and RDA is that it can get very sparse solutions. FTRL's algorithm is demonstrated as follows:

![FTRL](../img/ftrl_lr_project.png)

There are two ways to represent FTRL in Json:
```json
"optimizer": "ftrl",

"optimizer": {
    "type": "ftrl",
    "alpha": 0.1,
    "beta": 1.0,
    "reg1": 0.01,
    "reg2": 0.01
}
```
Note: ![](http://latex.codecogs.com/png.latex?\lambda_1,\lambda_2) are regularization coefficient corresponding to "reg1, reg2" in Json.

**Some reference experiences:** 

For wide & deep learning, in principle, it is necessary to ensure that the convergence speed of the two sides is not too different.
- Use FTRL as optimizer of the wide part as it converges relatively slowly
- Use Adam as optimizer of the deep part as it converges relatively quickly. Though the deep side uses a fast convergence optimizer, there are more parameters on deep side

FTRL optimizer is designed for online learning. Its amplitude of each update is very small in order to ensure the robustness of the model. In online learning cases, the data is fed row by row or minibatch by minibatch, so it's neither theoretically feasible to let a small size of data to modify the model too much. Therefore, the batch size shouldn't be too large when using FTRL, and had best be less than 10,000.

The convergence rate of different optimizers: FTRL < SGD < Momentum < AdaGrad ~ AdaDelta < Adam

Optimizers with diagonal approximation of Hessian, i.e. AdaGrad, AdaDelta, Adam, etc., allow larger batch sizes to gaurantee the gradient and accuracy of Hessian diagonal matrix. However, for simpler first-order optimizers such as FTRL, SGD and Momentum, more iterations are required, so the batch size shouldn't be too large. Therefore:
BatchSize(FTRL) < BatchSize(SGD) < BatchSize(Momentum) < BatchSize(AdaGrad) ~ BatchSize(AdaDelta) < BatchSize(Adam)

Regarding the learning rate, you can start from 1.0, and then increase or decrease it exponentially (with 2 or 0.5 as base). You can also use learning curve for early stop, but only when following these principles: SGD and Momentum can use relatively large learning rate, while AdaGrad, AdaDelta and Adam should generally use a less one since they are more sensitive to learning rage. (You can start from half of the learning rate of SGD and Momentum) 

Regarding to learning rate decay, it shouldn't be too large if there are less epochs. Use standard decay in general cases and WarnRestarts for AdaGrad, AdaDelta and Adam.

Regarding to regularization, FTRL, SGD, AdaGrad, AdaDelta currently supports L1/L2 regularization, while Momentum, Adam only supports L2 regularization. We recommend not using regularization at first but afterwards instead.

**Please refer to [optimizer](./optimizer.pdf) for derivation with L1 regularization.**