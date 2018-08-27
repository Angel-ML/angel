# Angel中的传递函数

## 1. Angel中的传递函数有:
名称| 表达式| 说明
---|---|---
Sigmoid | ![](http://latex.codecogs.com/png.latex?\frac{1}{1+e^{-x}}) | 将任意实数变成概率
tanh | ![](http://latex.codecogs.com/png.latex?\frac{e^{x}-e^{-x}}{e^{x}+e^{-x}}) | 将任意实数变成-1到1之间的数
relu | ![](http://latex.codecogs.com/png.latex?\max(0,x)) | 丢弃负的部分
dropout | ![](http://latex.codecogs.com/png.latex?rand()%20<%20\eta) | 随机置0
identity| ![](http://latex.codecogs.com/png.latex?x) | 原样输出

下图给出了一些常用的传递函数(部分Angel中没有提供):
![传递函数](../img/active_funcs.png)

## 2. 传递函数的json表示
### 2.1 无参数的传递函数
```json
"transfunc": "sigmoid",

"transfunc": {
    "type": "tanh"
}
```

### 2.2 有参数的传递函数
```json
"transfunc": "dropout",

"transfunc": {
    "type": "dropout",
    "proba": 0.5,
    "actiontype": "train"
}
```

注: 由于dropout传递函数在训练与测试(预测)中计算方式不一样, 所以要用actiontype表明是哪种场景, train/inctrain, predict.