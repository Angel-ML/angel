# Sync Controller

---

## Overview


In distributed systems, threads usually vary in their progresses; slow nodes can slow down the entire application by making other nodes wait for aggregation, and this can waste computational resource. 

For machine learning practice, in particular, the system can actually relax the synchronization restrictions: it is not necessary to wait for all nodes to complete their tasks for every iteration. Faster workers can push their computed increments sooner and proceed to the next iteration, thus reducing waiting time, making the entire application faster. This is why **Sync Controller** can be useful for distributed machine-learning systems. 

Angel provides three levels of sync control: **BSP (Bulk Synchronous Parallel)**，**SSP (Stalness Synchronous Parallel)** and **ASP (Asynchronous Parallel)**, among which, BSP is the most restricted synchronization protocol, whereas ASP is the least restricted. In general, more relaxed synchronization protocol results in better speed. 


![](../img/sync_controller.png)

## Introduction to the Sync Protocols

### 1. BSP
BSP is the default sync protocol in Angel and widely used in other distributed computing systems. It requires waiting of all tasks to complete in every iteration. 

 - Advantages: highly applicable; better convergence for each iteration
 - Disadvantages: waiting for the slowest task in each iteration, resulting in long time to complete the application
 - Use BSP in Angel: default setting
 
### 2. SSP
SSP allows the tasks to drift apart up to an upper limit, known as **staleness**, which is the number of iterations that the fastest task is allowed to be ahead of the slowest task. 

 - Advantages: reducing waiting time among tasks to some extent, resulting in better speed
 - Disadvantages: compared to BSP, SSP needs more iterations to reach the same level of convergence, also lacks applicability for some algorithms
 - Use SSP in Angel: configure `angel.staleness=N`, where **N** must be a positive integer
 
### 3. ASP

Tasks can drift apart with no restrictions; once a task finishes, it just continues to the next iteration without waiting.

- Advantages: eliminating the waiting time among tasks; fast
- Disadvantages: poor applicability; also, convergence is not guaranteed  
- Use ASP in Angel: configure `angel.staleness=-1`

Configuration of the sync controller is as simple as setting the `staleness` value, though one needs to keep in mind the tradeoff between convergence quality and speed, and it is always a good practice to adjust the sync control level, together with other related parameters, to the specific machine-learning algorithm and the metrics.


## Implementation Principle --- Vector Clock

In Angel, sync controllor is implemented using the *Vector Clock* algorithm.

![](../img/sync_controller_1.png)


### Procedure

 1. Maintain a vector clock for each partition on the server side, which records each worker's clock for its operations to this partition
 2. Maintain a background sync thread on the worker side to synchronize all partitions' clocks
 3. Task decides whether to wait to `get` (or other retrieving operations) from `PSModel` based on its local clock and staleness
 4. After each iteration, call the `clock` method of `PSModel` to update the vector clock

The default way to call the `clock` method is:

```Scala
	psModel.increment(update)
	……
	psModel.clock().get()
	ctx.incIteration()


```

Angel's versatile **sync controller** gives user convenient control over the synchronization protocols, making it possible to avoid serious performance issues in large-scale machine learning due to individual machine failures. 
