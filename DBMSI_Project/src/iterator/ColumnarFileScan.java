package iterator;


import bufmgr.PageNotReadException;
import diskmgr.Page;
import global.AttrType;
import global.PageId;
import global.RID;
import global.TID;
import heap.DataPageInfo;
import heap.FieldNumberOutOfBoundException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import heap.*;
import java.io.IOException;

/**
 * open a heapfile and according to the condition expression to get output file, call get_next to
 * get all tuples
 */
public class ColumnarFileScan extends Iterator {

	private AttrType[] _in1;
	private short in1_len;
	private short[] s_sizes;
	private Heapfile f;
	private Scan scan;
	private Tuple tuple1;
	private Tuple Jtuple;
	private int t1_size;
	private int nOutFlds;
	private CondExpr[] OutputFilter;
	public FldSpec[] perm_mat;
	int rowpos;
	public FldSpec[] temp_proj_list;


	/**
	 * constructor
	 *
	 * @param file_name heapfile to be opened
	 * @param in1 array showing what the attributes of the input fields are.
	 * @param s1_sizes shows the length of the string fields.
	 * @param len_in1 number of attributes in the input tuple
	 * @param n_out_flds number of fields in the out tuple
	 * @param proj_list shows what input fields go where in the output tuple
	 * @param outFilter select expressions
	 * @throws IOException some I/O fault
	 * @throws FileScanException exception from this class
	 * @throws TupleUtilsException exception from this class
	 * @throws InvalidRelation invalid relation
	 */
	public ColumnarFileScan(String file_name,
			AttrType in1[],
			short s1_sizes[],
			short len_in1,
			int n_out_flds,
			FldSpec[] proj_list,
			CondExpr[] outFilter
			)
					throws IOException,
					FileScanException,
					TupleUtilsException,
					InvalidRelation {

		_in1 = in1;
		in1_len = len_in1;
		s_sizes = s1_sizes;

		Jtuple = new Tuple();
		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] ts_size;
		ts_size = TupleUtils
				.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, s1_sizes, proj_list, n_out_flds);

		OutputFilter = outFilter;
		perm_mat = proj_list;
		temp_proj_list = proj_list;
		nOutFlds = n_out_flds;
		tuple1 = new Tuple();

		try {
			tuple1.setHdr(in1_len, _in1, s1_sizes);
		} catch (Exception e) {
			throw new FileScanException(e, "setHdr() failed");
		}
		t1_size = tuple1.size();

		try {
			f = new Heapfile(file_name); //relation.columnnum

		} catch (Exception e) {
			throw new FileScanException(e, "Create new heapfile failed");
		}

		try {
			scan = f.openScan();
		} catch (Exception e) {
			throw new FileScanException(e, "openScan() failed");
		}
	}

	/**
	 * @return shows what input fields go where in the output tuple
	 */
	public FldSpec[] show() {
		return perm_mat;
	}

	/**
	 * @return the result tuple
	 * @throws JoinsException some join exception
	 * @throws IOException I/O errors
	 * @throws InvalidTupleSizeException invalid tuple size
	 * @throws InvalidTypeException tuple type not valid
	 * @throws PageNotReadException exception from lower layer
	 * @throws PredEvalException exception from PredEval class
	 * @throws UnknowAttrType attribute type unknown
	 * @throws FieldNumberOutOfBoundException array out of bounds
	 * @throws WrongPermat exception for wrong FldSpec argument
	 */
	public Tuple get_next()
			throws JoinsException,
			IOException,
			InvalidTupleSizeException,
			InvalidTypeException,
			PageNotReadException,
			PredEvalException,
			UnknowAttrType,
			FieldNumberOutOfBoundException,
			WrongPermat {
		RID rid = new RID();
		TID tid = new TID(nOutFlds);
		Tuple nextTuple = new Tuple();
		
		
		while ((tuple1 = scan.getNext(rid)) != null) {
			tuple1.setHdr(in1_len, _in1, s_sizes);
			if (PredEval.Eval(OutputFilter, tuple1, null, _in1, null) == true) {
				Projection.Project(tuple1, _in1, Jtuple, perm_mat, nOutFlds);
				try {

					int curcount = rowpos;
					HFPage currentDirPage = new HFPage();
					Heapfile hf = new Heapfile("");
					PageId currentDirPageId = new PageId(hf._firstDirPageId.pid);

					PageId nextDirPageId = new PageId(0);


					boolean flag = true;

					// For moving between the directory pages
					while (currentDirPageId.pid != hf.INVALID_PAGE && flag) {
						hf.pinPage(currentDirPageId, currentDirPage, false);

						RID recid = new RID();
						Tuple atuple;

						// For moving within a directory page
						for (recid = currentDirPage.firstRecord(); recid != null; recid = currentDirPage.nextRecord(recid)) {
							atuple = currentDirPage.getRecord(recid);
							DataPageInfo dpinfo = new DataPageInfo(atuple);

							if( curcount - dpinfo.recct > 0){
								curcount -= dpinfo.recct;
							}
							else
							{
								flag = false;
								break;
							}
						}

						// ASSERTIONS: no more record
						// - we have read all datapage records on
						//   the current directory page.

						if(flag){
							nextDirPageId = currentDirPage.getNextPage();
							hf.unpinPage(currentDirPageId, false /*undirty*/);
							currentDirPageId.pid = nextDirPageId.pid;
						}
					}

					RID cur = currentDirPage.firstRecord();
					Scan sc = new Scan(hf);
					
					
					while(nextTuple != null && curcount >= 0){
						nextTuple = sc.getNext(cur);
						curcount--;
					}
					nextTuple.initHeaders();
					nextTuple.getStrFld(1); // get row tuple by adding a constructor

				} catch (Exception e) {
					System.out.println("Failed");
				}
			}
			rowpos++;
		}
		return nextTuple;
	}




	/**
	 * implement the abstract method close() from super class Iterator to finish cleaning up
	 */
	public void close() {

		if (!closeFlag) {
			scan.closescan();
			closeFlag = true;
		}
	}

}


