## psFunc Overall Design

---

According to the overall archetecture diagram, GetFunc and UpdateFunc are the most basic psf classes in Angel. We do not recommend inheriting them directly and writing self-defined psFunc from the scratch, though it is helpful to understand their functionalities and operations in order to create sensible and efficient psFunc. 

## GetFunc (Getting the Parameters)

Requests of the Get class go through three phases:

1. **Dividing the Request**
	
	* The parameter server interface manipulates the **entire model parameters**, and the model parameters are divided into multiple partitions to be stored on different PS instances; therefore, division needs to be done as early as the request stage
	
	* PS Client divides the request and generates a request list, where **every request corresponds to a model-parameter partition**

2. **Sending the Request** 
	* Angel sends every request in the request list to the PSServer with the corresponding model-parameter partition 

	* Each PSServer gets and updates parameters in units of model-parameter partitions and returns the result

3. **Merging the Result**
	*  All results at the model-partition level are merged to get the final result, whih is then returned 

## UpdateFunc (Updating the Parameters)

Requests of the Update class also go through three phases:

1. **Dividing the Request**

	PSClient divides the request and generates a request list where every request corresponds to a model-parameter partition 

2. **Sending the Request** 

	* Angel sends every request in the request list to the PSServer instance with the corresponding model-parameter partition 
	* The PS instance updates the parameters in units of model-parameter partitions
	
3. **Waiting for Completion**	
	* Wait for all requests to be completed and return

Whether it is the Get or Update class, the three phases can be customized to implement psFunc for various needs. 

