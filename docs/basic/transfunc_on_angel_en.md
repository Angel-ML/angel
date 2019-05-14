# Transfer Functions in Angel

## 1. Transfer Functions in Angel
Name| Expression | Description 
---|---|---
Sigmoid | ![](http://latex.codecogs.com/png.latex?\frac{1}{1+e^{-x}}) | Turn any real number into propability 
tanh | ![](http://latex.codecogs.com/png.latex?\frac{e^{x}-e^{-x}}{e^{x}+e^{-x}}) | Turn any real number into a number between -1 and 1 
relu | ![](http://latex.codecogs.com/png.latex?\max(0,x)) | Discard the negative part 
dropout | ![](http://latex.codecogs.com/png.latex?rand()%20<%20\eta) | Randomly set to zero 
identity| ![](http://latex.codecogs.com/png.latex?x) | Output as it is 

The following figure shows some common transfer functions (some of them are not available in Angel):
![传递函数](../img/active_funcs.png)

## 2. JSON Expression of Transfer Functions
### 2.1 Parameter-Free Transfer Functions
```json
"transfunc": "sigmoid",

"transfunc": {
    "type": "tanh"
}
```

### 2.2 Transfer Functions with Parameters
```json
"transfunc": "dropout",

"transfunc": {
    "type": "dropout",
    "proba": 0.5,
    "actiontype": "train"
}
```

Note: since the dropout transfer function is calculated differently in training and testing (prediction), users must specify in which scenario the function is used: train, inctrain or predict.