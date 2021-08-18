# DataSampling
## 1. Algorithm Introduction
This module is a commonly used method of data preprocessing and can usually be used as a prerequisite for other algorithms. It provides a method to randomly select a specific proportion or a specific number of small samples from the original data set.
Other common algorithm modules can complete the data sampling function by configuring the sampling rate without using this module separately; this module is often used to extract small samples for data visualization. <br>
Note: The final sampling ratio is min (sampling rate, sampling amount/total data amount). Therefore, if the sample size parameter is 1000, the final sample size may not be exactly 1000 <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, any data
- output: output, sampled data, the format is consistent with the input data
- sep: data separator, support: space (space), comma (comma), tab (\t)
- featureCols: indicates the column of the feature to be calculated, such as "1-10,12,15", 
which means that the feature is in the 1st to 10th column, 12th column and 15th column in the table, counting from 0

#### Algorithm parameters
- sampleRate: sample sampling rate
- takeSample: sample number, optional
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "DataSampling angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.DataSamplingExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 takeSample:25 \
  
```

# FillMissingValue
## 1. Algorithm Introduction
This module is to fill the empty values in the feature table, there are 4 filling methods: <br>
1.missingValue, fill in according to user-defined value <br>
2.mean, fill in according to the mean value <br>
3.median, fill in according to the median value <br>
4.count, fill in according to the mode <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, any data
- output: output, filled data, the format is consistent with the input data
- sep: data separator, support: space (space), comma (comma), tab (\t)

#### Algorithm parameters
- user-files: user configuration files, which define how missing values are filled
- fillStatPath: output the final missing value, fill method + "" + lable col + ":" + fill value, for example <br>
count 0:1 <br>
median 1:0.5 <br>
missingValue 2:888 <br>

user-files user configuration files, for example
```
# Json configuration file of sample data
 
{
    "feature": [
      {
        "id": "0",
        "fillMethod": "count"
      },
      {
        "id": "1",
        "fillMethod": "median"
       },
       {
        "id": "2-5",
        "missingValue": "888"
      }
    ]
}
```

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output
fillStatPath=hdfs://my-hdfs/fillStatPath

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "FillMissingValueExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --files ./localPath/FillMissingValueConf.txt \
  --class com.tencent.angel.spark.examples.cluster.FillMissingValueExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output fillStatPath:$fillStatPath sep:tab partitionNum:4 \
  user-files:FillMissingValueConf.txt \
  
```

# Spliter
## 1. Algorithm Introduction
This module divides the data set into two parts according to the fraction value, and stores the two parts separately <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, any data
- output1: data 1 after segmentation, the format is the same as the input data
- output2: data 2 after segmentation, the format is the same as the input data
- sep: data separator, support: space (space), comma (comma), tab (\t)

#### Algorithm parameters
- fraction: the data division ratio, a decimal between 0.0-1.0
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output1=hdfs://my-hdfs/output1
output2=hdfs://my-hdfs/output2

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "SpliterExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.SpliterExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output1:$output1 output2:$output2 sep:tab partitionNum:4 \
  fraction:0.8 \
  
```

# Dummy
## 1. Algorithm Introduction
There are two stages in the Dummy module, **feature cross** and **feature One-Hot**. ** Feature cross ** According to the json configuration file, cross the specified feature fields to generate the feature composed of feature name; ** Feature One-Hot ** Encode the feature name into a globally unified and continuous index. <br>
Algorithm does not involve ps related resources

## 2. Input data
The data input by the Dummy module is of the Table data type. <br>
```
// Sample data
1 0.5 2 1
0 0.4 3 2
1 0.5 1 1
```
Description: <br>
0 and -1 in the data are two special values, 0 means the default value, -1 means an illegal value; these two special values will be filtered out during the data reading process; therefore, the numerical representation of the characteristic should avoid 0 and- 1. <br>
Supports multi-value features. As the name implies, a multi-valued feature means that the feature can contain multiple values, and each value is divided by "|". For example, if a feature is a "favorite game", the value corresponding to the feature should be multiple game names, that is, a game list. <br>
The target field must be included. The target field of the training data is the label value of 0 and 1, and the target field of the prediction data is the identification id of each piece of data. <br>
Support standard libsvm data format, the first column is label, index and value are separated by colon. Multi-value features are not supported. <br>
```
// libsvm data format sample
1 3:0.4 5:0.6 6:10
0 1:0.1 2: 10 3:0.5
```

## 3. Feature crossover
The feature cross configuration file has two objects "fields" and "feature_cross". The "fields" object stores the name and index corresponding to each field of the input data; the "feature_cross" object is the configuration for generating features, where "id_features" refers to Generate a single feature, "comb_features" refers to cross features, dependencies refers to cross features, separated by commas, you can specify multiple. <br>
Description: <br>
Must contain the target field <br>
If a feature is a multi-value feature, "id_features" will generate multiple features, and "comb_features" will also cross other dependencies multiple times. <br>
The following are the intermediate results generated by the configuration of sample data and the feature crossover phase. <br>
```
# Json configuration file of sample data
{
  "fields": [
    {
      "name": "target",
      "index": "0"
    },
    {
      "name": "f1",
      "index": "1"
    },
    {
      "name": "f2",
      "index": "2"
    },
    {
      "name": "f3",
      "index": "3"
    }
  ],
  "feature_cross": {
    "id_features": [
      {
        "name": "f1"
      },
      {
        "name": "f3"
      }
    ],
    "comb_features": [
      {
        "name": "f1_f2",
        "dependencies": "f1,f2"
      },
      {
        "name": "f2_f3",
        "dependencies": "f2,f3"
      }
    ]
  }
}

```

```
// Intermediate results after feature crossover of sample data
1 f1_0.5 f3_1 f1_f2_0.5_2 f2_f3_2_1
0 f1_0.4 f3_2 f1_f2_0.4_2 f2_f3_3_2
1 f1_0.5 f3_1 f1_f2_0.5_1 f2_f3_1_1
```

## 4. Features One-Hot
Feature One-Hot is based on the intermediate result after feature crossover, replacing the feature name string with a globally unified and continuous feature index. <br>
Generate sample data in dummy format. Each sample is separated by a comma. The first element is the target field (the label of the training data or the sample ID of the predicted data).
Other fields refer to non-zero feature indexes. <br>
```
// The result after one hot
1,0,2,4,7
0,1,3,5,8
1,0,2,6,9
```

## 5. Running example
#### Algorithm IO parameters

- input: input, any data
- output: output. The output of feature Dummy contains three directories, featureCount, featureIndexs, and instances. Output path, when instanceDir, indexDir, countDir do not exist, automatically generate path
- sep: data separator, support: space (space), comma (comma), tab (\t)
- baseFeatIndexPath: The mapping from feature name to index used when feature One-Hot is specified. It is an optional parameter. When you need to do incremental update or predict the dummy of the data based on the result of the last feature dummy, you need to specify this parameter to ensure that the index of the two dummy is the same.
- user-files: configuration files
- instanceDir: save the sample data in dummmy format after One-Hot
- indexDir: saves the mapping relationship between feature name and feature index, separated by ":";
- countDir: save the feature space dimension after dummy, only save an Int value,

#### Algorithm parameters
- countThreshold: When the frequency of a feature in the entire data set is less than the threshold, it will be filtered out. Generally set to about 5
- negSampleRate: In most application scenarios, the proportion of negative samples is too large, and this parameter can be used to sample negative samples
- artitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "DummyExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.DummyExample \
  --files ./localPath/featConfPath \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 user-files:featConfPath \
  negSampleRate:1 countThreshold:5 \
  
```

# Correlation
## 1. Algorithm Introduction
This module obtains the correlation between the features by calculating the Pearson or Spearman correlation coefficient between the two features. The correlation calculation mode of this module is mainly divided into two types: <br>
One is to calculate the correlation between the two features with unknown correlation (hereinafter referred to as the new feature), the result is a diagonal matrix, and the elements of each diagonal matrix are the correlation coefficients between the two features. The larger the value, the stronger the correlation between these two features; <br>
The other is to calculate the correlation between the new feature and the feature with known correlation (hereinafter referred to as the old feature). Note: When the variance of a certain column feature is 0, the calculated correlation value with the feature is NaN. <br>
Algorithm does not involve ps related resources <br>

Output: <br>
1. If no new feature column is specified, the module does not perform any calculations and does not output any results <br>
2. If only the new feature column is specified, and the old feature column is not specified, only the correlation between the new features will be calculated. The output format is as in the following example (1) <br>
3. If both the new feature column and the old feature column are specified, the module will not only calculate the correlation between the new features, but also calculate the correlation between the new feature and the old feature column. The output format is as follows (2) in the text example <br>
Here are a few examples to illustrate the above situation: <br>
(1) If only the new feature column is specified: the output is the correlation between the new features. For example, if the new feature is listed as 1, 2, 3, the output format is: <br>

```
# Sample correlation coefficient output
X 1 2 3
1 1.0 0.15 0.25
2 0.15 1.0 0.38
3 0.25 0.38 1.0
```
The above data shows that there are a total of three features to calculate the correlation between two. The first row and the first column respectively display the Id of the new feature (X can be ignored), and the other elements are the correlation coefficients between the new features. The elements are separated by spaces. <br>
(2) Both the new feature and the old feature are designated: the output is a combination of two correlation matrices, which are the correlation matrix between the new features and the correlation matrix between the new feature and the old feature. For example, the new feature is 1, 2, 3, and the old feature is 4, 5, 6, 7, and the output format is: <br>
```
# Sample correlation coefficient output
X 1 2 3 4 5 6 7
1 1.0 0.15 0.25 0.57 0.15 0.25 0.02
2 0.15 1.0 0.38 0.15 0.11 0.38 0.49
3 0.25 0.38 1.0 0.25 0.38 0.03 0.21
```
In the above data, the first line is the Id of the new feature and the old feature (X can be ignored), the first column is the Id of the new feature, and the other elements are between the two new features and between the new feature and the old feature The correlation coefficient.

## 2. Running example
#### Algorithm IO parameters

- input: input, feature input file
- output: output, correlation coefficient output
- sep: data separator, support: space (space), comma (comma), tab (\t)
- newColStr: The column where the new feature is located, such as "1-10,12,15", which indicates that the feature is in the 1st to 10th column, 12th column and 15th column in the table, counting from 0
- oriColStr: The column where the old feature is located, such as "1-10,12,15", which indicates that the feature is in the 1st to 10th, 12th and 15th columns in the table, counting from 0

#### Algorithm parameters
- sampleRate: sample sampling rate
- method: the method of correlation calculation, divided into two types: pearson and spearman, blank means using pearson method
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "CorrelationExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.CorrelationExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 newColStr:1-5 oriColStr:7-9 method:pearson \
  
```

# MultualInformation
## 1. Algorithm Introduction
This module uses mutual information formulas to calculate the correlation between features. The larger the value, the stronger the correlation between features. The principle and calculation formula of mutual information can be referred to. Same as the PearsonOrSpearman module, the correlation calculation of the MultualInformation module is mainly divided into two types: one is to calculate the correlation between the new features, and the result is a diagonal matrix. The element of each diagonal matrix is the correlation coefficient between the two features. The larger the value, the stronger the correlation between the two features; the second is to calculate the correlation between the new feature and the old feature separately. <br>
Output: The output format is the same as the output of Correlation, but the diagonal element in the correlation coefficient matrix between the two new features is the information entropy of the feature, and the other elements are the mutual information between the features. <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, feature input file
- output: output, mutual information output
- sep: data separator, support: space (space), comma (comma), tab (\t)
- newColStr: The column where the new feature is located, such as "1-10,12,15", which indicates that the feature is in the 1st to 10th column, 12th column and 15th column in the table, counting from 0
- oriColStr: The column where the old feature is located, such as "1-10,12,15", which indicates that the feature is in the 1st to 10th, 12th and 15th columns in the table, counting from 0

#### Algorithm parameters
- sampleRate: sample sampling rate
- takeSample: sample number, optional
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "MutualInformationExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.MutualInformationExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:0.8 newColStr:1-5 oriColStr:7-9
  
```


# Discrete
## 1. Algorithm Introduction
Discrete algorithm discretizes the characteristic data. Discrete methods include equal frequency and equal value discrete methods. The equal frequency discrete method divides the eigenvalues into corresponding buckets according to the order of eigenvalues from small to large, and the number of elements contained in each bucket is the same; equivalent discrete method The division boundary of each bucket is determined according to the minimum and maximum value of the characteristic value, so as to ensure that the division width of each bucket is numerically equal. <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, any data
- output: output, sampled data, the format is consistent with the input data
- sep: data separator, support: space (space), comma (comma), tab (\t)
- disBoundsPath: The storage path of the discrete feature boundary, the format is: an array of feature id + boundary (the boundary is separated by a space)


#### Algorithm parameters
- sampleRate: sample sampling rate
- partitionNum: the number of data partitions, the number of spark rdd data partitions
- user-files: feature configuration file name

featureConfName: Feature configuration file name, upload the configuration file from tesla page. The following is an example of a feature configuration file in JSON format for this module:
```
{
    "feature": [
      {
        "id": "2",
        "discreteType": "equFre",
        "numBin": "3"
      },
      {
	"id": "5",
        "discreteType": "equVal",
        "numBin": "3",
	"min": "0",
	"max": "100"
       },
       {
        "id": "0",
        "discreteType": "equVal",
        "numBin": "2",
	"min":"-1"
      }
    ]
}
```
The above is to configure the 3 features. There is no requirement for the order between feature configurations. If some features do not require discrete configuration, they are not written in the configuration. The "feature" in the configuration file cannot be changed, the user only needs to modify the following parameters: <br>
Feature configuration parameters: <br>
"id": Represents the feature Id, note that the Id is the column number of the feature in the input data, counting from 0 <br>
"discreteType": the type of discretization, "equFre" means equal frequency discrete, "equVal" means equal discrete <br>
"numBin": The number of discretized buckets. Please note that in the equal frequency discretization method, if the number of buckets is set too large and the number of elements in each bucket is too small, resulting in repeated points in the discrete boundary, an error will occur.< br>
"min": For the equivalent discrete configuration, the minimum value of the characteristic value is limited. If the characteristic value is smaller than this value, an error will occur. If there is no need, leave it blank, same as "max" below <br>
"max": For the equivalent discrete configuration, the maximum value of the characteristic value is limited. If the characteristic value is larger than this value, an error will occur. If there is no need, you can leave it blank

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output
disBoundsPath=hdfs://my-hdfs/disBoundsPath


source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "DiscretizeExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --files ./localPath/DiscreteJson.txt \
  --class com.tencent.angel.spark.examples.cluster.DiscretizeExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output disBoundsPath:$disBoundsPath sep:tab partitionNum:4 \
  sampleRate:1 user-files:DiscreteJson.txt \
  
```

# Information Based
## 1. Algorithm Introduction
Based on the feature selection of information, the module includes 4 algorithms: Information Gain, Gini, Information Gain Ratio and Symmetry Uncertainly <br>
Algorithm does not involve ps related resources <br>

Input: Table data <br>
Output: feature importance matrix, if the feature to be calculated importance is 1, 2, 3, the output format is: <br>
```
# Feature importance matrix
X IGR GI MI SU
1 0.03 0.04 0.2 0.07
2 0.15 0.018 0.38 0.009
3 0.25 0.33 0.025 0.17
```
The first line represents the feature importance calculation index (X can be ignored), IGR represents the information gain rate, GI represents the Gini coefficient, MI represents the information gain, and SU represents the symmetric uncertainty. The remaining rows represent the feature id (that is, the column number of the feature in the input data, starting from 0) and the result corresponding to each indicator.
## 2. Running example
#### Algorithm IO parameters

- input: input, Table data with target label column
- output: output, sampled data, the format is consistent with the input data
- sep: data separator, support: space (space), comma (comma), tab (\t)
- featureCols: indicates the column of the feature to be calculated, such as "1-10,12,15", which means that the feature is in the 1st to 10th column, 12th column and 15th column in the table, counting from 0
- labelCol: The column where the target label is located, counting from 0 according to the position of the target label in the table


#### Algorithm parameters
- sampleRate: sample sampling rate
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "InfoComputeExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.InfoComputeExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:1 labelCol:0 featureCols:1-10 \
  
```

# RandomizedSVD
## 1. Algorithm Introduction
The RandomizedSVD algorithm is based on the principle of the paper "Finding structure with randomness: Probabilistic algorithms for constructing approximate matrix decompositions" and the matrix SVD decomposition algorithm implemented on the spark platform. <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, labeled data, data with row labels, each row of data represents the row label of the matrix and the value of the row in the matrix
- outputS: The output path of the row vector representation of all eigenvalues, that is, the row vector composed of the main diagonal elements of the singular value matrix, and the output path arranged in descending order
- outputV: The storage path of the right singular vector V, labeled data, each line starts with the corresponding line label
- outputU: The save path of the left singular vector U, labeled data, each line starts with the corresponding line label
- sep: data separator, support: space (space), comma (comma), tab (\t)
- featureCols: indicates the column of the feature to be calculated, such as "1-10,12,15", which means that the feature is in the 1st to 10th column, 12th column and 15th column in the table, counting from 0
- labelCol: the column where the label is located, counting from 0


#### Algorithm parameters
- sampleRate: sample sampling rate
- K: the number of singular values
- rCond: reciprocal condition number
- iterationNormalizer: Iterative methods, divided into two types: "QR" and "none", "QR" method means that QR decomposition is required for each iteration, and "none" means that only the left multiplication of matrices A and A is performed in each iteration. Transpose, no "QR" decomposition process in the middle
- qOverSample: sampling parameter q
- numIteration: The number of iterations, if it is auto, the final number of iterations is if (k <(min(matRow, matCol) * 0.1)) 7 else 4
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
outputS=hdfs://my-hdfs/outputS
outputV=hdfs://my-hdfs/outputV
outputU=hdfs://my-hdfs/outputU

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "RandomizedSVDExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.RandomizedSVDExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input outputS:$outputS outputU:$outputU outputV:$outputV sep:tab partitionNum:4 \
  sampleRate:1 iterationNormalizer:QR numIteration:3 qOverSample:1 K:2 abelCol:0 rCond:1e-9 \
  
```

# Scaler
## 1. Algorithm Introduction
The Scaler module integrates two methods: MinMaxScaler and StandardScaler. The user can specify the normalization method of a feature through the feature configuration file. The two methods are described below: <br>
The MinMaxScaler algorithm performs a unified normalization process on the feature data. The default normalized feature value range is [0,1], and the user can also specify the normalized value range to be [min,max]. <br>
The calculation formula for this normalization is: <br>
```
((x-EMin)/(EMax-EMin))*(max-min)+min
```
Where x represents the feature value that needs to be normalized, EMin represents the minimum value under the feature, EMax represents the maximum value under the feature, and min and max are the normalized value ranges set by the user. Note that when the maximum and minimum values of a column of features are equal, all values in the column are normalized to 0.5*(max-min) + min.
The StandardScaler algorithm mainly standardizes the features. The original feature data will be transformed into a new feature with a variance of 1 and a mean of 0 through the transformation of the algorithm. The calculation formula is:
```
((x-Mean)/Var)
```
Among them, Mean represents the average value of the feature, and Var represents the sample standard deviation of the feature. The following special circumstances should be noted: <br>
(1) If Var is 0, the normalized result of x is directly 0.0 <br>
(2) No need for averaging processing: At this time, the algorithm only does variance processing, that is: x/Var <br>
(3) No variance processing is required: x takes the value directly (x-Mean) <br>


Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters

- input: input, feature table
- output: output, normalize the corresponding features on the basis of the original input data format unchanged
- standardPath: save the mean and variance of features, optional
- sep: data separator, support: space (space), comma (comma), tab (\t)
- user-files: feature configuration file name
- scaleConfPath: feature configuration file name, upload configuration file from tesla page. The following is a sample feature configuration file of the module in JSON format <br>
```
{
    "minmax":[
      {
        "colStr": "1-2",
        "min": "0",
	"max":"1"
      },
      {
	"colStr":"5",
        "min":"-1",
        "max": "1"
       }
       ],
	"standard":[
       {
        "colStr": "3,6-7",
        "std": "true",
	"mean":"false"
       },
       {
        "colStr": "8,9",
	"std":"true",
	"mean":"true"
       }
       ]
}

```

Feature configuration parameters: <br>
"minmax" and "standard": respectively represent the corresponding normalization modules MinMaxScaler and StandardScaler <br>
"colStr": The id of the feature that needs to be processed accordingly. The value is counted from 0 according to the column of the feature in the original table. Multiple features can be separated by ",", and "-" can also be used to identify the start to end columns of the feature, for example, "1-20" means column 1 to column 20. <br>
"min": The minimum value after normalization <br>
"max": The maximum value after normalization <br>
"std": Do you need to standardize the variance? <br>
"mean": Do you need to standardize the mean? <br>

#### Algorithm parameters
- sampleRate: sample sampling rate
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "ScalerExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --files ./localPath/scaleConf.txt \
  --class com.tencent.angel.spark.examples.cluster.ScalerExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output sep:tab partitionNum:4 \
  sampleRate:1 user-files:scaleConf.txt \
  
```
# Reindex
## 1. Algorithm Introduction
Re-index the node id of the graph, generate an incremental new node id starting from 0, return the re-indexed side file and the mapping relationship between the nodes before and after the re-index <br>
Algorithm does not involve ps related resources
## 2. Running example
#### Algorithm IO parameters
- input: input, network structure input, supports hdfs/tdw path, each line represents an edge. When the input is hdfs, it must be in the two-column format of "src separator dst". When the input is tdw, you need to specify the number of columns where src/dst is located. The default is 0/1. If the input is tdw, the input table is both It can be numeric type src(long) | dst(long) | weight(float) or string type src(string) | dst(string) | weight(string)
- srcIndex: the column where the src node is located
- dstIndex: the column where the dst node is located
- weightIndex: indicates the column where the weight is located
- isWeighted: Whether the input is a weighted graph
- output: output, re-indexed side file, 2 or 3 columns, support tdw and HDFS; if the output table is tdw, the user needs to create the table first, and the format can be either a numeric type or a string type, src(long) | dst(long) | weight(float) or src(string) | dst(string) | weight(string)
- maps: re-indexed mapping dictionary, 2 columns, the first column is old id, the second column is new id; supports tdw and HDFS; if it is tdw output, the user needs to create a table first, and the format can be either a numeric type or a string Type, oldid | newid
- sep: data separator, support: space (space), comma (comma), tab (\t)


#### Algorithm parameters
- partitionNum: the number of data partitions, the number of spark rdd data partitions

#### Submitting scripts

```
input=hdfs://my-hdfs/data
output=hdfs://my-hdfs/output
maps=hdfs://my-hdfs/maps

source ./spark-on-angel-env.sh
$SPARK_HOME/bin/spark-submit \
  --master yarn-cluster\
  --name "ReindexExample angel" \
  --jars $SONA_SPARK_JARS  \
  --driver-memory 5g \
  --num-executors 1 \
  --executor-cores 4 \
  --executor-memory 10g \
  --class com.tencent.angel.spark.examples.cluster.ReindexExample \
  ../lib/spark-on-angel-examples-3.2.0.jar \
  input:$input output:$output maps:$maps sep:tab partitionNum:4 \
  srcIndex:0 dstIndex:1 weightIndex:2 isWeighted:false \
  
```
