# Data Set For UT & Examples

## [a9a](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#a9a) (bin-class)
- classes: 2
- data: 32,561 / 16,281 (testing)
- features: 123

#### 1. LR
- loss=0.342
- precision=0.842
- auc=0.899
- trueRecall=0.562
- falseRecall=0.938


## [census](https://archive.ics.uci.edu/ml/datasets/Census+Income) (bin-class)
- classes: 2
- data: 22,792 / 9,769 (testing)
- features: 148
- field number: 13

#### 1. LR
- loss=0.333
- precision=0.850
- auc=0.900
- trueRecall=0.581
- falseRecall=0.938

#### 2. FM
- loss=0.334
- precision=0.850
- auc=0.900
- trueRecall=0.573
- falseRecall=0.940

#### 3. Deep FM
- loss=0.323
- precision=0.853
- auc=0.904
- trueRecall=0.595
- falseRecall=0.937

#### 4. Deep & Wide
- loss=0.323
- precision=0.853
- auc=0.904
- trueRecall=0.595
- falseRecall=0.936

#### 5. NFM
- loss=0.322
- precision=0.853
- auc=0.906
- trueRecall=0.597
- falseRecall=0.936

### 6. PNN
- loss=0.322
- precision=0.853
- auc=0.905
- trueRecall=0.597
- falseRecall=0.936

## [w6a](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/binary.html#w6a) (bin-class)
- classes: 2
- data: 12,292 / 8,595 (testing)
- features: 300

#### 1. LR
- loss=0.076
- precision=0.980
- auc=0.968
- trueRecall=0.676
- falseRecall=0.988

#### 2. DNN
- loss=0.057
- precision=0.985
- auc=0.982
- trueRecall=0.826
- falseRecall=0.990

## [protein](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#protein) (multi-class/clustering)
- classes: 3
- data: 14,895 / 6,621 (testing)
- features: 357


## [usps](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html#usps) (multi-class/clustering)
- classes: 10
- data: 7,291 / 2,007 (testing)
- features: 256


## [abalone](https://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/regression.html#abalone) (regression)
- data: 4,177
- features: 8


## [agaricus](https://github.com/dmlc/xgboost/tree/master/demo/data)
- classes: 2
- data: 6,513 / 1,611 (testing)
- features: 127



## UT Results

## LinearRegTest
- dataset: abalone
- epoch: 100
- batchsize: 100
- learnrate: 0.25
- decay: 2
- reg: 0.0001
- R-Square: 0.4743

## RobustRegTest
- dataset: abalone
- epoch: 100
- batchsize: 100
- learnrate: 2
- decay: 1
- reg: 0.0001
- R-Square: 0.4517

## KmeansTest
- dataset: usps
- epoch: 50
- batchsize: 3000
- C: 0.3
- sample to center average dist: 5.72
