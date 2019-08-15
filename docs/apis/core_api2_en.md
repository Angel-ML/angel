# Angel Core APIs
Angel abstracts two interfaces, AngelClient and MatrixClient. AngelClient is responsible for launching the Angel PS, and MatrixClient is responsible for interacting with Angel PS.

## AngelPSClient

* ```AngelContext startPS() throws AngelException```  Startup Angel PSs
* ```void stop() throws AngelException``` Stop Angel PSs
* ```void createMatrices(List<MatrixContext> contexts) throws AngelException``` Create matrices in PSs
* ```void load(ModelLoadContext context) throws AngelException``` Read model that stored in the files to PSs
* ```void save(ModelSaveContext context) throws AngelException``` Save the model in PSs to the files

* ```void checkpoint(int checkpointId, ModelSaveContext context) throws AngelException```  Write checkpoints for the model store in PSs
* ```void recover(int checkpointId, ModelLoadContext context) throws AngelException``` Recover the model from the checkpoint

## MatrixClient
MatrixClient is used to pull model from PSs and push model updates to PSs, Most of the methods defined in MatrixClient have both synchronous and asynchronous versions. The synchronous method representation method call will be blocked until that the execution is complete, and the asynchronous method will immediately return a Future object.

 * ```void increment(int rowId, Vector row) throws AngelException``` Plus a row vector to the matrix row

* ```Future<VoidResult> asyncIncrement(int rowId, Vector row) throws AngelException``` Plus a row vector to the matrix row

* ```void increment(int[] rowIds, Vector[] rows) throws AngelException``` Plus the row vectors to the matrix rows
* ```Future<VoidResult> asyncIncrement(int[] rowIds, Vector[] rows) throws AngelException``` Plus the row vectors to the matrix rows
* ```void update(int rowId, Vector row) throws AngelException``` Update the matrix row use the row vector
* ```Future<VoidResult> asyncUpdate(int rowId, Vector row) throws AngelException``` Update the matrix row use the row vector
* ```void update(int[] rowIds, Vector[] rows) throws AngelException``` Update the matrix rows use the row vectors
* ```Future<VoidResult> asyncUpdate(int[] rowIds, Vector[] rows) throws AngelException```  Update the matrix rows use the row vectors
* ```Vector get(int rowId, int[] indices) throws AngelException``` Get the matrix row elements use the column indices, it is used in **INT** key matrix
* ```Future<Vector> asyncGet(int rowId, int[] indices) throws AngelException``` Get the matrix row elements use the column indices, , it is used in **INT** key matrix
* ```Vector get(int rowId, long[] indices) throws AngelException``` Get the matrix row elements use the column indices, it is used in **LONG** key matrix
* ```Future<Vector> asyncGet(int rowId, long[] indices) throws AngelException``` Get the matrix row elements use the column indices, it is used in **LONG** key matrix
* ```Vector[] get(int[] rowIds, int[] indices) throws AngelException``` Get the matrix rows elements use the column indices, it is used in **INT** key matrix
* ```Future<Vector[]> asyncGet(int[] rowIds, int[] indices) throws AngelException``` Get the matrix rows elements use the column indices, it is used in **INT** key matrix
* ```Vector[] get(int[] rowIds, long[] indices) throws AngelException``` Get the matrix rows elements use the column indices, it is used in **LONG** key matrix
* ```Future<Vector[]> asyncGet(int[] rowIds, long[] indices) throws AngelException``` Get the matrix rows elements use the column indices, it is used in **LONG** key matrix
* ```GetResult get(GetFunc func) throws AngelException``` Use PS Function to get data from PSs, it is an open interface, developers can implement any  "pull" function by extending GetFunc and GetResult 
* ```Future<GetResult> asyncGet(GetFunc func) throws AngelException``` Use PS Function to get data from PSs, it is an open interface, developers can implement any  "pull" function by extending GetFunc and GetResult 
* ```void update(UpdateFunc func) throws AngelException``` Use PS Function to push data to PSs, it is an open interface, developers can implement any  "push" function by extending UpdateFunc 
* ```Future<VoidResult> asyncUpdate(UpdateFunc func) throws AngelException``` Use PS Function to push data to PSs, it is an open interface, developers can implement any  "push" function by extending UpdateFunc 
* ```Vector getRow(int rowId) throws AngelException``` Get the row of the matrix
* ```Future<Vector> asyncGetRow(int rowId) throws AngelException``` Get the row of the matrix
* ```GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException``` Get the rows use pipleline mode
* ```Vector [] getRows(int [] rowIds) throws AngelException``` Get the rows of the matrix

## Angel PSF(PS Function)
Angel PSF is use to extend PS function. With PSF, developers can not only customize the PS RPC interface they need, but also optimize the calculation process and push some calculations to the PS to save the network. Communication bandwidth. There are two type PSFs: pull and push.

Angel PSF basic process:
![][1]


### Pull PSF: GetFunc
GetFunc is the base class of all pull type psfs, it define like this:
``` 
public abstract class GetFunc {
   protected final GetParam param;
   public GetFunc(GetParam param) {
     this.param = param;
   }
   public GetParam getParam() {return param;}
   public abstract PartitionGetResult partitionGet(PartitionGetParam partParam);
   public abstract GetResult merge(List<PartitionGetResult> partResults);
 }
 ```

Pull PSF main steps
* Worker(PSClient) Split the user request to ps requests (GetParam has a split method)
* Worker Send the requests to the PSs
* PS handle the ps request, send the result to Worker(partitionGet method in GetFunc)
* Worker merge the results of ps requests, return the final result(merge method in GetFunc)

### Push PSF: UpdateFunc
UpdateFunc is the base class of all pull type psfs, it define like this:
```public abstract class UpdaterFunc {
 	private final UpdaterParam param;
 	public UpdaterFunc(UpdaterParam param) {
   		this.param = param;
 	  }
 	public UpdaterParam getParam() {return param;}
 	public abstract void partitionUpdate(PartitionUpdaterParam partParam);
 }
 ```
Push PSF main steps
* Worker(PSClient) Split the user request to ps requests (UpdaterParam has a split method)
* Worker Send the requests to the PSs
* PS handle the ps request, send the result to Worker(partitionGet method in GetFunc)
* Worker waiting all results of the ps request

[1]: ../img/pull_psf.png
