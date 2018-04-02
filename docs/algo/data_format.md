# Angel数据格式

## 1 libsvm格式

每行文本表示一个样本，每个字段以" "(空格)分隔，每行的文本格式
```
label index1:value1 index2:value1 index3:value3 ...
```

其中
* label字段：
  - 字段类型：Int
  - 当输入数据是训练数据，label为样本的标签，二分类算法label是{0, 1}，多分类算法label是{0, 1, 2, ..., n} ；
  - 当输入数据是预测数据，label是样本的index；
* index:value字段：
  - 特征index对应的value，index类型为Int，value类型为Double
  - 特征的index，从1开始计数, 与标准的libsvm格式一致

```
# libsvm样例数据
 1 1:0.5 3:3.1 7:1.0
 0 2:0.1 3:2.3 5:2.0
 1 4:0.2 7:1.1 9:0.0
    ....
```

## 2 dummy格式

每一行为一条记录(一个样本)，每个字段以" "分隔，每行的文本格式
```
"label index1 index2 index3"
```
* label字段
  - 字段类型：Int
  - 当输入数据是训练数据，label为样本的标签，二分类算法label是{0, 1}，多分类算法label是{0, 1, 2, ..., n} ；
  - 当输入数据是预测数据，label是样本的index；
* index字段
  - 字段类型：Int/Long
  - 特征的index，从0开始计数
  - 这些是特征值为1的index，其他的就是特征值为0的index

```
# 数据格式样例
0 3 7 999 666
1 0 2 88 77
  ...
```

如果输入数据的分隔符不是空格, 可以用如下参数来指定分隔符, 如指定主逗号:
> ml.data.splitor=,

