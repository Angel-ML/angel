# Angel Data Types

## 1 libsvm

A text format where each line represents a labeled feature vector using the following format:
```
label index1:value1 index2:value1 index3:value3 ...
```

* label
  - type: Int
  - when the input is training data, it is the label of the data point; for binary classification, a label should be {0, 1}; for multiclass classification, labels should be class indices starting from zero: {0, 1, ..., n}
  - when the input is test data, it is the index of the data point
* index:value
  - represents the feature's index-value pair, where index type is Int and value type is Double
  - feature index starts from 1, similar to libsvm' style


```
# libsvm example
 1 1:0.5 3:3.1 7:1.0
 0 2:0.1 3:2.3 5:2.0
 1 4:0.2 7:1.1 9:0.0
    ....
```

## 2 dummy

A text format where each line represents a labeled feature vector, separated by ` `(space) using the following format:
```
label index1 index2 index3
```

* label
  - type: Int
  - when the input is training data, it is the label of the data point; for binary classification, a label should be {0, 1}; for multiclass classification, labels should be class indices starting from zero: {0, 1, ..., n}
  - when the input is test data, it is the index of the data point
* index
  - type: Int/Long
  - feature index, starting from 0
  - represents the indices of features that are 1 (unrepresented features are 0)

```
# dummy type example
0 3 7 999 666
1 0 2 88 77
  ...
```

Note: if the row splitor is not ` `(space), you can specify a splitor (say ",") by using the following option:
> ml.data.splitor=,
