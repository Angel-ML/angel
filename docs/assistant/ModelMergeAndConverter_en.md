### ModelMergeAndConverter

---

> Both `ModelMergeAndConverter` and `ModelConverter` can convert model files from binary format to text format. The difference is that,  `ModelConverter` produces multiple model files as results, whereas `ModelMergeAndConverter` will merge the model partitions at first, and output the results to a single file. Since `ModelMergeAndConverter` needs to merge the model into a whole in-memory, and the same file cannot be written in concurrency, `ModelMergeAndConverter` will generally take more time in terms of conversion as well as I/O. Hence, we recommend using `ModelMergeAndConverter` only when the model is not extremely large.

## Model File Format before Conversion

The default design of Angel's model files is as follows:

* Each model has a source file "meta" that holds the model's parameters and partition information, as well as index information of each partition data
* Multiple model files, each of which holds one or more partition data

## Conversion Command
Angel currently supports two modes to start up a conversion tool: the **client mode** and the **Angel task mode**.

The client mode is to perform a conversion task directly on the machine that runs the script (usually the client machine that submits tasks). The Angel task mode, on the other hand, is to launch a Angel Yarn Job, and execute the conversion program with Angel's Worker as the job's container. It is generally recommended to use the client mode, which is simpler to use. However, you can choose to use Angel task mode if the client resources are limited, since the conversion process will require considerable CPU and network IO resources (especially when the model is extremely large).

The command to submit a Angel ModelConverter job is:

### Client Mode

```bsh
./bin/angel-model-mergeconvert \
--angel.load.model.path ${anywhere} \
--angel.save.model.path ${anywhere} \
--angel.modelconverts.model.names ${models} \
--angel.modelconverts.serde.class ${SerdeClass}
```
### Angel Task Mode
```bsh
./bin/angel-submit \
--angel.app.submit.class com.tencent.angel.ml.toolkits.modelconverter.ModelMergeAndConverterRunner \
--angel.load.model.path ${anywhere} \
--angel.save.model.path ${anywhere} \
--angel.modelconverts.model.names ${models} \
--angel.modelconverts.serde.class ${SerdeClass}
```

* **Parameter Descriptions**:
    * **angel.load.model.path**  
      Model input path, namely the save path of the original binary model file. Note that it is not required to include the matrix name in path
    * **angel.save.model.path**   
      Save path of the converted text file
    * **angel.modelconverts.model.names**   
      List of the names of models to be converted, separated by ",". **This parameter is not required. The converter will convert all models under the designated directory path if this parameter is not assigned**
    * **angel.modelconverts.serde.class**    
      Serialization format of row output. **This parameter is not required. The default "index:value" format will be used when the parameter is not assigned**
    * **angel.app.submit.class**    
      Starting entrance of an Angel task, configured as `com.tencent.angel.ml.toolkits.modelconverter.ModelMergeAndConverterRunner`

### File Format after Conversion

* Converted models are stored in rows

* As demonstrated in the example below, head of the file holds the model's basic information (name, dimension, etc.). Each row's index will be identified as "rowIndex=xxx" at the beginning, followed by a series of key:value pair, with key and value respectively indicating the column index under this row and its corresponding value.

    ```
    rowIndex=0
    0:-0.004235138405748639
    1:-0.003367253227582031
    3:-0.003988846053264014
    6:0.001803243020660425
    8:1.9413353447408782E-4
    ...
    ```

