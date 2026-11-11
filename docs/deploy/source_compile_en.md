# Angel Compilation Guide

---

1. **Source Code Download**

	```git clone https://github.com/Tencent/angel```
2. **Compile**

   2.1. **Compile Manually**

    2.1.1. **Compiling Environment Dependencies**

    * JDK 1.8.
    * Maven 3.x; this build was verified with 3.9.6.
    * Protobuf 2.5.0. `protoc` must be on `PATH` and match the project's `protobuf-java` and Hadoop dependencies.
    * Build dependencies: Scala 2.12.15, Spark 3.3.1 and Hadoop 2.7.3. Runtime validation used Spark 3.3.4 and Hadoop 2.10.2.

    2.1.2. **Compile Instructions**

    Run the following from the repository root:

    ```bash
    mvn clean package -Dmaven.test.skip=true
    ```

    The distribution `angel-${version}-bin.zip` is generated under `dist/target`. This command skips tests; a successful build does not mean every algorithm has been verified.

   2.2. **Compile By Docker**

       If there is a docker environment, you can directly execute the script ```sh docker-build.sh``` to compile.
       After compilied, a distribution package named 'angel-{version}-bin.zip'  will be generated under `dist`

3. **Distribution Package**

    Unpacking the distribution package, the following subdirectories will be generated under the root directory:
    * bin: contains submit scripts
        * `spark-on-angel-env.sh`, `SONA-example`: environment settings and an example submit script for Spark on Angel
        * `angel-cluster-up`, `angel-standalone-submit`, `angel-ps-launch` (the first two also have `.bat` versions for Windows): scripts for the STANDALONE mode, see [STANDALONE / EMBEDDED / Attach Deployment](standalone_deploy_en.md)
    * conf: contains system config files
    * data: contains data for testing
    * jsons: contains json config examples of Angel algorithms
    * lib: contains jars for Angel and dependencies

    STANDALONE still needs Hadoop Java dependencies even without a running YARN service. The current distribution's Hadoop 2.x jars may conflict with `hadoop-client-api` / `hadoop-client-runtime`. Use a conflict-free classpath for direct local execution and independent PS startup; see [Local Running](local_run_en.md) and [Other Deployment Options](standalone_deploy_en.md).
