# Transfer function in Angel

## 1. Transfer function in Angel:
name| expression| description
---|---|---
Sigmoid | ![](http://latex.codecogs.com/png.latex?\frac{1}{1+e^{-x}}) | turn any real number into probability
tanh | ![](http://latex.codecogs.com/png.latex?\frac{e^{x}-e^{-x}}{e^{x}+e^{-x}}) | turn any real number into a number between -1 and 1
relu | ![](http://latex.codecogs.com/png.latex?\max(0,x)) | discard the negative part
dropout | ![](http://latex.codecogs.com/png.latex?rand()%20<%20\eta) | randomly set to 0
identity| ![](http://latex.codecogs.com/png.latex?x) | original output
The following figure shows some common transfer functions (some not available in Angels):
![trans_func](../img/active_funcs.png)

## 2. The json representation of the transfer function
### 2.1 transfer function without parameters
```json
"transfunc": "sigmoid",

"transfunc": {
    "type": "tanh"
}
```

### 2.2 transfer function with parameters
```json
"transfunc": "dropout",

"transfunc": {
    "type": "dropout",
    "proba": 0.5,
    "actiontype": "train"
}
```

ps: Since the dropout transfer function in training and testing (prediction) is not calculated in the same way, it is necessary to use actiontype to indicate the type of scene, train/inctrain, predict.