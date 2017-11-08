### ModelConverter

---

> When Angel application is complete, any PSModel will be saved in binary format for enhancing speed and saving space. Understanding that some users may need to have the PSModels saved in plaintext format, we created ModelConverter class to parse the PSModel binary file to plaintext.

## Format Before Conversion (Binary)

Before conversion, Angel's binary model file has the following default design:

* Each model has a source file "meta" that holds information of the model parameters and partitions, as well as indexing information for each partition
* There are multiple model files where each file holds data of one or more model partitions

## Format After Conversion (Plaintext)

After conversion, Angel's plaintext model file has the following default design:

* A model is saved in units of partitions, where each output file may contain one or more partitions; the correspondence of the partitions to the output file is the same as to the original binary file
* Format of the converted file is shown below. Mark "rowIndex=xxx" at the beginning of each row, followed by a sequence of key:value pairs, where key and value are the column index and value in the corresponding row. **Please note that a row may be divided to multiple partitions, thus a complete row may be stored as multiple segments in multiple files**

```
rowIndex=0
0:-0.004235138405748639
1:-0.003367253227582031
3:-0.003988846053264014
6:0.001803243020660425
8:1.9413353447408782E-4
```

## Converter

Currently, Angel supports two converter modes: **stand-alone mode** and **distributed mode**. 

### 1. Stand-alone Mode

Conversion is done on the machine that runs the script (typically, the gateway machine that is used to submit the Angel job). In general, this mode is easy to use thus is recommended. The submit command is shown below:

```bsh
./bin/angel-model-convert \
--angel.load.model.path ${hdfs_path} \
--angel.save.model.path ${hdfs_path} \
--angel.modelconverts.model.names ${models} \
--angel.modelconverts.serde.class ${SerdeClass}
```

### 2. Distributed Mode

Even though the stand-alone mode is easy to use, it is subject to the gateway machine's resource and can have poor performance due to the consumption of CPU and network IO, especially when the model is large. The distributed mode is provided to address this issue.

The distributed mode starts an Angel job under the Yarn mode, where conversion is done within the worker, taking advantage of Angel's distributed processing capability. This mode puts only small pressure on the gateway machine and improves speed. The submit command is shown below: 

```bsh
./bin/angel-submit \
--angel.app.submit.class com.tencent.angel.ml.toolkits.modelconverter.ModelConverterRunner \
--angel.load.model.path ${hdfs_path} \
--angel.save.model.path ${hdfs_path} \
--angel.modelconverts.model.names ${models} \
--angel.modelconverts.serde.class ${SerdeClass}
```

**Parameters:**

* angel.load.model.path  
	* Model input path, i.e. the save path for model in its original binary format (**this path does not need to include the matrix name**)
* angel.save.model.path   
	* Save path for converted model in text format
* angel.modelconverts.model.names   
	* List of names of models that need to be converted; can specify multiple model names separated by ","
	* **Note**: this parameter is not required; when unspecified, all models under the specified input path will be converted
* angel.modelconverts.serde.class
	* Serialization format of the model output lines
	* **Note**: this parameter is not required; when unspecified, the default line format is "index:value"

