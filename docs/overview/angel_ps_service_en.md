# Angel PS Service
ParameterServer is a very general architecture. In the process of big data analysis and calculation, it is often necessary to store and exchange a large number of parameters. At this time, you can use ParameterServer.  Angel supports the PS Service working mode: when starting the Angel application, only the Angel PS and Angel Master are started,  Angel PS is responsible for the storage and exchange of parameters, and other systems are responsible for calculation.

Angel abstracts two interfaces, AngelClient and MatrixClient. AngelClient is responsible for launching the Angel PS, and MatrixClient is responsible for interacting with Angel PS.

So far, we have built two systems based on Angel PS Service: Spark On Angel and Pytorch On Angel.

The architecture  of Spark On Angel is shown below.
![][1]

When Spark needs to store a large number of parameters in the calculation process, it can use AngelClient to start the Angel PS in Spark Driver, and then use MatrixClient to interact with Angel's PS in Spark Executors.

[1]: ../img/spark_on_angel_architecture2.png
