# Local Mode

Local mode is mainly used for testing and debugging. Currently, local mode support only one worker (allowing multiple tasks) and one PS. Configure `angel.deploy.mode` to run local mode; detailed parameter configurations can be found in [Angel System Parameters](./config_details_en.md)

## 1. Execution Environment Requirements

* Hadoop >= 2.2.0
* Java version 1.8
* Angel distribution package angel-&lt;version&gt;-bin.zip

First, configure HADOOP_HOME and JAVA_HOME, and unzip Angel distribution package. Angel jobs can run on local now. 

## 2. LOCAL Examples

Once the distribution package is unzipped, find the `bin` directory under the root, and that's where all the submit scripts are located. An example running simple logistic regression can be found at:

```./angel-example com.tencent.angel.example.ml.SGDLRLocalExample```
