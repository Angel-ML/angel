# Running Angel On Kubernetes

Angel can also run on clusters managed by [Kubernetes](https://kubernetes.io/). This feature makes use of native Kubernetes scheduler that has been added to Angel.

### 1. **Preparing for the run environment**

The environment requirements for running Angel on Kubernetes include:

* A runnable distribution of Angel 3.0.0 or above.

* A running Kubernetes cluster at version >= 1.6 with access configured to it using kubectl. If you do not already have a working Kubernetes cluster, you may set up a test cluster on your local machine using [minikube](https://kubernetes.io/docs/setup/learning-environment/minikube/).
  * If use minikube, we recommend using the latest release of minikube with the DNS addon enabled.
  * Be aware that the default minikube configuration is not enough for running Angel applications. We recommend 1 CPUs and 2g of memory for per angel ps/worker to be able to start a example.

* You must have appropriate permissions to list, create, edit and delete pods in your cluster. You can verify that you can list these resources by running kubectl auth can-i <list|create|edit|delete> pods.
  * The service account credentials used by the Angel Master pods must be allowed to create pods, services and configmaps.

* You must have Kubernetes DNS configured in your cluster.

### 2. **Angel on Kubernetes Job Example**

* Docker images  
  Kubernetes requires users to supply images that can be deployed into containers within pods. The images are built to be run in a container runtime environment that Kubernetes supports. Docker is a container runtime 
  environment that is frequently used with Kubernetes. We provide a basic image for the user, you can pull it down and run or customize your own image through it. Follow the pull command:  
  `docker pull tencentangel/angel:v3.0.0`

* Distributed File System  
  we both support cephfs and hdfs distributed file system, to ensure that the kubernetes cluster's pods are accessible

* Namespace && [RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/)
  * Create a namespace for angel, namespaces are ways to divide cluster resources between multiple users (via resource quota). Angel on Kubernetes can use namespaces to launch Angel applications. This can be made use of 
  through the `angel.kubernetes.namespace` configuration. Follow the command to create a namespace:  
  `kubectl create namespace angel-group`
  * Create a service account, the Angel master pod uses a Kubernetes service account to access the Kubernetes API server to create and watch parameter server and worker pods. the service account must be granted a Role or 
  ClusterRole that allows angel master pod to create pods and services. Follow the command to create service account:  
  `kubectl create serviceaccount angel`
  * To create a RoleBinding or ClusterRoleBinding, grant a service account a Role or ClusterRole. The following command creates an edit ClusterRole in the angel-group namespace and grants it to the angel service account 
  created above:  
  `kubectl create clusterrolebinding angel-role --clusterrole=edit --serviceaccount=angel-group:angel --namespace=angel-group`

* **Submitting the Job**
  * data preparation  
  Put your data to cephfs or hdfs, angel example data in `$ANGEL_HOME/data`. This example uses `$ANGEL_HOME/data/a9a_123d_train.libsvm`  as data, assuming the data has been uploaded to cephfs.
    
  * Use `angel-submit` under the `bin` directory in the distribution package to submit Angel jobs to the Kubernetes cluster  
  > **please make sure the cluster has enough resources; for the following example, at least 9GB memory and 5 vcores are needed to start the job**
  
     ```$xslt
     export ANGEL_HOME=/path/to/angel 
     $ANGEL_HOME/bin/angel-submit \
              -Dangel.deploy.mode=KUBERNETES \
              -Dangel.kubernetes.master=<api_server_url> \
              -Dangel.app.submit.class=com.tencent.angel.ml.core.graphsubmit.GraphRunner \
              -Daction.type=train \
              -Dangel.kubernetes.container.image=tencentangel/angel:v3.0.0 \
              -Dangel.kubernetes.master.volumes.hostPath.test.mount.path=/angel/mountpath \
              -Dangel.kubernetes.master.volumes.hostPath.test.options.path=/cephfs/basepath/to/angel \
              -Dangel.kubernetes.executor.volumes.hostPath.test.mount.path=/angel/mountpath \
              -Dangel.kubernetes.executor.volumes.hostPath.test.options.path=/cephfs/basepath/to/angel \
              -Dangel.kubernetes.namespace=angel-group \
              -Dangel.kubernetes.serviceaccount=angel \
              -Dangel.ps.number=2 \
              -Dangel.ps.memory.gb=2 \
              -Dangel.am.memory.gb=1 \
              -Dangel.workergroup.number=2\
              -Dangel.worker.memory.gb=2 \
              -Dangel.train.data.path=file:///angel/mountpath/to/data \
              -Dangel.save.model.path=file:///angel/mountpath/to/model \
              -Dangel.tmp.output.path.prefix=/angel/mountpath/to/tmp \
              -Dangel.output.path.deleteonexist=true \
              -Dangel.log.path=file:///angel/mountpath/to/angel_log \
              -Dangel.job.name=angel_lr_test \
              -Dml.epoch.num=5 \
              -Dml.model.class.name=com.tencent.angel.ml.classification.LogisticRegression \
              -Dml.feature.index.range=123 \
              -Dml.model.size=123 \
              -Dml.data.validate.ratio=0.1 \
              -Dml.data.type=libsvm \
              -Dml.learn.rate=0.01 \
              -Dml.reg.l2=0.03 \
              -Dml.inputlayer.optimizer=ftrl
     ```
     while run on kubernetes, `angel.deploy.mode` must set `KUBERNETES` and `angel.kubernetes.master` must set kubernetes 
     cluster api server url, If you have a Kubernetes cluster setup, one way to discover the apiserver URL is by executing kubectl cluster-info.
     ```$xslt
     kubectl cluster-info
     Kubernetes master is running at http://127.0.0.1:8080
     ```
     When using the cephfs, you need to mount path to ensure angel master/worker can access data in cephfs.
     
### 3. **Other Features**

* Dependency Management

  Application dependencies can be pre-mounted into custom-built Docker images. Those dependencies can be added to the classpath 
  by referencing them with / use `angel.kubernetes.master.extraClassPath` and `angel.kubernetes.executor.extraClassPath` in angel-submit script 
  or setting the ANGEL_EXTRA_CLASSPATH environment variable in your Dockerfiles. For example, if you want use hdfs, then your images need to contain hadoop 
  client and hadoop dependency.
  
* Using Kubernetes Volumes

  Users can mount the following types of Kubernetes [volumes](https://kubernetes.io/docs/concepts/storage/volumes/) into the Angel master and ps/worker pods:
  * hostPath: mounts a file or directory from the host node’s filesystem into a pod.
  * emptyDir: an initially empty volume created when a pod is assigned to a node.
  * persistentVolumeClaim: used to mount a PersistentVolume into a pod.
  
  To mount a volume of any of the types above into the Angel master pod, use the following configuration property:
  ```$xslt
  --conf angel.kubernetes.master.volumes.[VolumeType].[VolumeName].mount.path=<mount path>
  --conf angel.kubernetes.master.volumes.[VolumeType].[VolumeName].mount.readOnly=<true|false>
  ```
  Specifically, VolumeType can be one of the following values: hostPath, emptyDir, and persistentVolumeClaim. VolumeName is the name you want to use for the 
  volume under the volumes field in the pod specification. For example, the path of a hostPath with volume name test can be specified using the following property:
  
  ```$xslt
  angel.kubernetes.master.volumes.hostPath.test.options.path=/cephfs/basepath/to/angel
  ```
  The configuration properties for mounting volumes into the Angel ps/worker pods use prefix `angel.kubernetes.executor.` instead of `angel.kubernetes.master.`. 
  
* Logs & Debug
  * Accessing Logs  
  Logs can be accessed using the Kubernetes API and the kubectl CLI. When a Angel application is running, it’s possible to stream logs from the application using:
  ```$xslt
  kubectl -n <namespace> logs -f <angel-master/ps/worker-pod-name>
  ```
  The same logs can also be accessed through the `Kubernetes dashboard` if installed on the cluster.
  
  * Debug  
  To get some basic information about the scheduling decisions made around the Angel master pod, you can run:  
    ```$xslt
    kubectl -n <namespace> describe pod <angel-master-pod>
    kubectl -n <namespace> get pod <angel-master-pod> -o yaml --export
    ```
    If the pod has encountered a runtime error, the status can be probed further using:  
    ```$xslt
    kubectl -n <namespace> logs <angel-master-pod>
    ```

### 4. **Configuration**

The following configurations are specific to Angel on Kubernetes.

| Property Name    | Default  | Meaning |
| --- | --- | --- |
| angel.kubernetes.master | none | kubernetes cluster api server url |
| angel.kubernetes.namespace  | default | The namespace that will be used for running the angel master and angel ps/worker pods. |
| angel.kubernetes.container.image | none | Container image to use for the Angel application. This is usually of the form example.com/repo/angel:v3.0.0. This configuration is required and must be provided by the user, we provide a base image `docker pull tencentangel/angel:v3.0.0` . |
| angel.kubernetes.container.image.pullPolicy | IfNotPresent | Container image pull policy used when pulling images within Kubernetes, valid values are `Always`, `Never`, and `IfNotPresent`. |
| angel.kubernetes.serviceaccount | default | Service account that is used when running the angel master pod. The angel master pod uses this service account when requesting angel ps/worker pods from the API server |
| angel.kubernetes.master.label.[LabelName] | none | Add the label specified by LabelName to the angel master pod. |
| angel.kubernetes.master.annotation.[AnnotationName] | none | Add the annotation specified by AnnotationName to the angel master pod. |
| angel.kubernetes.executor.label.[LabelName] | none | Add the label specified by LabelName to the angel ps/worker pod. |
| angel.kubernetes.executor.annotation.[AnnotationName] | none | Add the annotation specified by AnnotationName to the angel ps/worker pod. |
| angel.kubernetes.masterEnv.[EnvironmentVariableName] | none | Add the environment variable specified by EnvironmentVariableName to the Angel master process. The user can specify multiple of these to set multiple environment variables. |
| angel.kubernetes.executorEnv.[EnvironmentVariableName] | none | Add the environment variable specified by EnvironmentVariableName to the Angel ps/worker process. The user can specify multiple of these to set multiple environment variables. |
| angel.kubernetes.master.extraClassPath | none | Add the extra classpath to the angel master process |
| angel.kubernetes.executor.extraClassPath | none | Add the extra classpath to the angel ps/worker process |
| angel.kubernetes.master.volumes.[VolumeType].[VolumeName].mount.path | none | Add the Kubernetes Volume named VolumeName of the VolumeType type to the angel master pod on the path specified in the value. |
| angel.kubernetes.master.volumes.[VolumeType].[VolumeName].mount.readOnly | none | Specify if the mounted volume is read only or not. |
| angel.kubernetes.master.volumes.[VolumeType].[VolumeName].options.[OptionName] | none | Configure Kubernetes Volume options passed to the Kubernetes with OptionName as key having specified value, must conform with Kubernetes option format. |
| angel.kubernetes.executor.volumes.[VolumeType].[VolumeName].mount.path | none | Add the Kubernetes Volume named VolumeName of the VolumeType type to the angel ps/worker pod on the path specified in the value. |
| angel.kubernetes.executor.volumes.[VolumeType].[VolumeName].mount.readOnly | none | Specify if the mounted volume is read only or not. |
| angel.kubernetes.executor.volumes.[VolumeType].[VolumeName].options.[OptionName] | none | Configure Kubernetes Volume options passed to the Kubernetes with OptionName as key having specified value, must conform with Kubernetes option format. |
| angel.kubernetes.master.secrets.[SecretName] | none | Add the Kubernetes Secret named SecretName to the angel master pod on the path specified in the value. |
| angel.kubernetes.executor.secrets.[SecretName] | none | Add the Kubernetes Secret named SecretName to the angel ps/worker pod on the path specified in the value. |

All angel on kubernetes configuration information can be referred to `AngelConf` class.  

**Note**: If you want to make your own image from the base image we provided, the `entrypoint.sh` and `Dockerfile` sample in source code `kubernetesmanager/scripts`.