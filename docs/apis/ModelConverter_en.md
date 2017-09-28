### ModelConverter

---

>  Angel saves PSModel in binary format after the application is completed. The ModelConverter class parses the PSModel binary file to plaintext format.

Each partition is saved as a file under the name partitionID. All the partitions of a PSModel are saved to the same directory; the name of the directory is the same as the modelName field of PSModel. The ModelConverter job can be submitted using the following command:

```bsh
./bin/angel-submit \
-- action.type train \
-- angel.app.submit.class com.tencent.angel.ml.toolkits.modelconverter.ModelConverterRunner \
-- ml.model.in.path ${modelInPath}
-- ml.model.name ${PSModelName}
-- ml.model.out.path ${modelOutPath} \
-- ml.model.convert.thread.count ${threadCount} \
-- angel.save.model.path ${anywhere} \
-- angel.workergroup.number 1 \
-- angel.worker.memory.mb 1000  \
-- angel.worker.task.number 1 \
-- angel.ps.number 1 \
-- angel.ps.memory.mb 500 \
-- angel.job.name ${jobname}
```

### **Parameters**

* **ml.model.in.path** 
	* Model input path, corresponding to the specified `ml.model.out.path`

* **ml.model.name** 
	* Name of the model to be parsed (PSModel.modelName)

* **ml.model.out.path** 
	* Output path of the model after being parsed into plaintext

* **angel.save.model.path** 
	* A save path must be set for this parameter when submitting an Angel application, but the value will be emptied, and there will be no output when the application is completed.

* **angel.workergroup.number** 
	* Recommended value is 1 for this parameter; model parsing is done by one worker by default

* **angel.worker.task.number**
	* Recommended value is 1; model parsing is done with one task by default

* **ml.model.convert.thread.count** 
	* Number of threads for model converting

## **Format of the converted file**

* Angel model is saved in units of partitions, where each partition is saved as one file under the name partitionID. Each converted model file correpsonds to a model partition.
* In the following: the two values in the first row are `rowID` and `clock`, respectively; each row from the second line shows a key:value pair of the model 

 ```
0, 10 
0:-0.004235138405748639
1:-0.003367253227582031
3:-0.003988846053264014
6:0.001803243020660425
8:1.9413353447408782E-4
```
