package columnar;

import java.io.IOException;

import global.AttrType;
import global.Convert;
import global.RID;
import global.TID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import value.IntegerValue;
import value.StringValue;
import value.ValueClass;

public class ColumnarFile {

	public String columnarFileName;
	public int numberOfColumns;
	public AttrType[] attributeType;
	public int stringSize;

	public int tupleLength;
	public int deleteCount;
	public String [] heapFileNames;
	public String [] columnNames;
	public Heapfile[] heapFileColumns;
	public Heapfile columnarFile;
	public Heapfile deletedTupleList;

	public ColumnarFileMetadata cfm;


	public ColumnarFile() {
		// TODO Auto-generated constructor stub	
	}
	
	public ColumnarFile(String name, int numberOfColumns, AttrType[] type, int stringSize) {

		this.columnarFileName = name;
		this.attributeType = type;
		this.numberOfColumns = numberOfColumns;

		this.tupleLength = 0;
		this.deleteCount = 0;
		this.stringSize = stringSize;

		heapFileNames = new String [numberOfColumns];
		heapFileColumns = new Heapfile[numberOfColumns];
		columnNames = new String [numberOfColumns];

		int i = 0;

		try {

			for (AttrType t: type) {
				heapFileNames[i] = name.concat(Integer.toString(i));
				heapFileColumns[i] = new Heapfile(heapFileNames[i]);

				if(t.attrType == AttrType.attrInteger) {
					tupleLength = tupleLength + 4;
				}
				else if (t.attrType == AttrType.attrString) {
					tupleLength = tupleLength + stringSize;
				}
				i++;
			}

			cfm = new ColumnarFileMetadata(this);
			columnarFile = new Heapfile(name+".hdr");
			cfm.columnarFileName = name;

		} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




	}

	public void deleteColumnarFile() {
		try {
			for (Heapfile h: this.heapFileColumns) {
				h.deleteFile();
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public TID insertTuple(byte[] tuplePtr) {

		int curPos = 0;
		TID tid = new TID(numberOfColumns);
		tid.recordIDs = new RID[numberOfColumns];

		int i = 0;
		try {
			for (AttrType t: attributeType) {
				if (t.attrType == AttrType.attrInteger)	{

					int intAttr = Convert.getIntValue(curPos,tuplePtr);
					curPos = curPos + 4;

					byte[] value = new byte[4];
					Convert.setIntValue(intAttr, 0, value);

					tid.recordIDs[i] = new RID();
					tid.recordIDs[i] = heapFileColumns[i].insertRecord(value);
				}
				if (t.attrType == AttrType.attrString)	{

					String strAttr = Convert.getStrValue(curPos,tuplePtr,stringSize);

					curPos = curPos + stringSize;

					byte[] value = new byte[stringSize];
					Convert.setStrValue(strAttr, 0, value);

					tid.recordIDs[i] = new RID();
					tid.recordIDs[i] = heapFileColumns[i].insertRecord(value);
				}

				i++;
			}
			tid.numRIDs = i;
			//tid.position = hfColumns[0].RidToPos(tid.recordIDs[0]);

		}
		catch (Exception e)	{
			e.printStackTrace();
		}
		return tid;

	}

	public Tuple getTuple(TID tid) {

		byte[] tuple = new byte[tupleLength];
		int offset = 0;

		Tuple t = new Tuple();

		try {
			for (int i= 0; i < numberOfColumns ; i++) {

				t = heapFileColumns[i].getRecord(tid.recordIDs[i]);

				if (attributeType[i].attrType == AttrType.attrInteger) {				
					int value = Convert.getIntValue(offset,t.returnTupleByteArray());
					Convert.setIntValue(value,offset,tuple);
					offset = offset + 4;
				}

				if (attributeType[i].attrType == AttrType.attrString) {
					String value = Convert.getStrValue(offset,t.returnTupleByteArray(),stringSize);
					Convert.setStrValue(value,offset,tuple);
					offset = offset + stringSize;
				}
			}

			t.tupleSet(tuple,0,tuple.length);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return t;

	}

	public ValueClass getValue(TID tid, int column) {

		ValueClass value = null;

		IntegerValue i= new IntegerValue();
		StringValue str= new StringValue();

		try{
			byte[] colValue = heapFileColumns[column].getRecord(tid.recordIDs[column]).returnTupleByteArray();

			if (attributeType[column].attrType == AttrType.attrInteger)	{

				i.setValue(Convert.getIntValue(0,colValue));
				value = i;
			}
			else if (attributeType[column].attrType == AttrType.attrString)	{

				str.setValue(Convert.getStrValue(0,colValue,stringSize));
				value = str;
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		return value;

	}

	public int getTupleCnt() {

		int count = 0;
		try {
			count = heapFileColumns[0].getRecCnt();
		} catch (InvalidSlotNumberException | InvalidTupleSizeException | HFDiskMgrException | HFBufMgrException
				| IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return count;
	}

	public TupleScan openTupleScan() {

		TupleScan scan = new TupleScan(this);
		return scan;

	}

	public Scan openColumnScan(int columnNo) {

		Scan scan = null;
		try {
			scan = new Scan(heapFileColumns[columnNo]);
		} catch (InvalidTupleSizeException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return scan;

	}

	public boolean updateTuple(TID tid, Tuple newtuple) {

		int i = 0;

		for (;i<numberOfColumns;i++) {
			if(!updateColumnofTuple(tid,newtuple,i+1))
				return false;
		}
		return true;

	}

	public boolean updateColumnofTuple(TID tid, Tuple newtuple, int column) {

		int intValue;
		String strValue;
		Tuple tuple = null;
		try {
			if (attributeType[column-1].attrType == AttrType.attrInteger)	{
				intValue = newtuple.getIntFld(column);
				tuple = new Tuple(4);
				tuple.setIntFld(1, intValue);
			}
			else if (attributeType[column-1].attrType == AttrType.attrString)	{
				strValue = newtuple.getStrFld(column);
				tuple = new Tuple(stringSize);
				tuple.setStrFld(1, strValue);
			}

			return heapFileColumns[column-1].updateRecord(tid.recordIDs[column-1], tuple);

		}catch (Exception e)	{
			e.printStackTrace();
		}
		return false;

	}

	public boolean createBTreeIndex(int column) {
		return true;

	}

	public boolean createBitMapIndex(String dbname, String columnarFile, int columnNo, ValueClass value) {

		//BitMapFile(dbname, columnarFile, columnNo, value);
		return true;

	}

	public boolean markTupleDeleted(TID tid) {

		byte[] deletedTids = new byte[numberOfColumns*4*2];

		int i = 0;
		int offset = 0;
		int tidOffset = 0;


		try{
			for (AttrType attr: attributeType) {
				if(attr.attrType == AttrType.attrInteger)
				{
					Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
					Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

					offset = offset + 4;
					tidOffset = tidOffset + 8;
					if(!heapFileColumns[i].deleteRecord(tid.recordIDs[i]))
						return false;
					i++;
				}
				else if(attr.attrType == AttrType.attrString)
				{
					Convert.setIntValue(tid.recordIDs[i].pageNo.pid, tidOffset, deletedTids);
					Convert.setIntValue(tid.recordIDs[i].slotNo, tidOffset + 4, deletedTids);

					offset = offset + stringSize;
					tidOffset = tidOffset + 8;
					if(!heapFileColumns[i].deleteRecord(tid.recordIDs[i]))
						return false;
					i++;
				}
			}
			deletedTupleList.insertRecord(deletedTids);
			this.deleteCount++;
			return true;
		}catch (Exception e)
		{
			e.printStackTrace();
		}
		return false;

	}

	public boolean purgeAllDeletedTuples() {
		return false;


	}

	public Heapfile getHeapfileForColumname(String columnName) {
		int i = 0;
		boolean found = false;
		for (String columnname : columnNames) {
			if (columnName.equalsIgnoreCase(columnname)) {
				found = true;
				break;
			}
			i++;
		}
		if (found)
			return heapFileColumns[i];
		else return null;
	}
	
	public int getColumnNumberFromColumname(String columnName) {
		int i = 0;
		boolean found = false;
		for (String columnname : columnNames) {
			if (columnName.equalsIgnoreCase(columnname)) {
				found = true;
				break;
			}
			i++;
		}
		if (found)
			return i;
		else return 0;
	}

	public void setColumnarFileMetadata(int stringSize) {
		try  {

			cfm.stringSize = stringSize;
			for(int i = 0 ; i < numberOfColumns ; i++)	{
				cfm.heapFileNames[i] = heapFileNames[i];
				cfm.attributeType[i] = attributeType[i].attrType;
				cfm.columnNames[i] = columnNames[i];
			}
			cfm.getTuple();
			columnarFile.insertRecord(cfm.data);
		}
		catch (Exception e)	{
			e.printStackTrace();
		}
	}

	public ColumnarFileMetadata getColumnarFileMetadata (String name) {
		if(name != null) {

			ColumnarFileMetadata cfm = null;
			try {

				Heapfile hf = new Heapfile(name);
				RID rid = new RID();
				Tuple tuple = new Tuple();
				Scan s = hf.openScan();
				tuple = s.getNext(rid);
				cfm = new ColumnarFileMetadata();
				cfm.stringSize = stringSize;
				cfm.getColumnarFileMetadata(tuple);
			}
			catch (Exception e) {
				e.printStackTrace();
			}  
			return cfm;
		}
		else {			  
			return null;
		}	  
	}


}
