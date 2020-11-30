# Loss Functions in Angel

## 1. Loss Functions in Angel
Angel includes a lot of loss functions, which can be divided into two categories:

- Loss functions for classification
- Loss functions for regression

Loss functions for classification in Angel include:

Name|Expression|Description
---|---|---
CrossEntropyLoss| ![](http://latex.codecogs.com/png.latex?-\sum_iy_i\log{p_i(x)}) | Cross entropy loss is used for classification (logistic regression is a special case). You can also use multiclass Softmax. Cross entropy loss requires the input to be the propability ![](http://latex.codecogs.com/png.latex?p_i(x)) instead of ![](http://latex.codecogs.com/png.latex?f(x)) 
LogLoss| ![](http://latex.codecogs.com/png.latex?\log{\(1+e^{-yf(x)}\)}) | Log loss is used for classification, and is the loss function of logistic regression. Log loss can be regarded as a special case of CrossEntropyLoss, which materializes ![](http://latex.codecogs.com/png.latex?p_i(x)) using sigmoid function 
SoftmaxLoss| ![](http://latex.codecogs.com/png.latex?-\sum_iI(y=i)\log\frac{x^{x_i}}{\sum_je^{x_j}}) | A special form of CrossEntoryLoss, which materializes ![](http://latex.codecogs.com/png.latex?p_i(x)) using sigmoid function 
HingeLoss| ![](http://latex.codecogs.com/png.latex?\max{\(0,1-yf(x)\)}) | Loss function of SVM 

Graphically represented as follows:

![分类损失函数](../img/classifcationloss.png)

Loss functions for regression in Angel include:

Name|Expression|Description
---|---|---
L2Loss | ![](http://latex.codecogs.com/png.latex?\|y-f(x)\|_2^2) | L2 loss is used for regression, and is the loss function of OLS 
HuberLoss | ![](http://latex.codecogs.com/png.latex?\left\\{\begin{array}{ll}\delta\cdot\(abs(x)-\frac{\delta}{2}\),&abs(x)>\delta\\\\\frac{1}{2}x^2,&abs(x)\le\delta\end{array}\right.) | Huber loss is used for regression. It uses a quadratic function near 0 and a linear function in the other domain. It solves the problem that hte absolute value function is not conductive near zero. The models derived using Huber loss are more robust 

The graphical representation is as follows:

![回归损失函数](../img/regressionloss.png)

## 2. Implementaion of Loss in Angel
All loss functions in Angel implement LossFunc Trait:

```scala
trait LossFunc extends Serializable {
  def calLoss(modelOut: Matrix, graph: AngelGraph): Double

  def loss(pred: Double, label: Double): Double

  def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix

  def predict(modelOut: Matrix): Matrix
}
```
It is obvious that the loss functions in Angel can not only compute the loss but also compute gradients and predict. It is because of the gradient calculation implemented in `LossFunc` that the back propagation has a starting point. Calculating gradients in `LossLayer` is directly invoking the `calGrad` method in `LossFunc` :

```scala
  override def calGradOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
        gradOutput = lossFunc.calGrad(output, graph)
        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    gradOutput
  }
```
Another funcion is `predict`. Calculating the prediction results in `LossLayer` is directly invoking the `predict` method in `LossFunc`: 
```scala
  override def predict(): Matrix = {
    status match {
      case STATUS.Null =>
        calOutput()
      case _ =>
    }

    lossFunc.predict(output)
  }
```

## 3. JSON Configuration of Loss Functions
### 3.1 Loss Functions without Parameters
All loss functions have no parameter except Huber loss. For these parameter-free loss functions, there are two ways of expression in JSON:

```json
"lossfunc": "logloss"

"lossfunc": {
    "type": "logloss"
} 
```

### 3.2 Loss Functions with Parameters
Only Huber loss requires parameters. The JSON configuration is as follows:
```json
"lossfunc": {
    "type": "huberloss",
    "delta": 0.1
}
```
