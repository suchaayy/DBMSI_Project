package columnar;

import java.io.IOException;

import global.Convert;
import heap.Tuple;

public class ColumnarFileMetadata {

	public String columnarFileName;
	public int numberOfColumns;
	public int tupleLength;
	public int stringSize;
	public int[] attributeType;	
	public String[] heapFileNames;	
	public String[] columnNames; 
	public byte[] data;
	public static int size;

	public ColumnarFileMetadata() {
		// TODO Auto-generated constructor stub
	}

	public ColumnarFileMetadata(ColumnarFile cf) {
		// TODO Auto-generated constructor stub

		this.columnarFileName = cf.columnarFileName;
		this.numberOfColumns = cf.numberOfColumns;
		this.tupleLength = cf.tupleLength;
		this.stringSize = cf.stringSize;
		this.attributeType = new int[numberOfColumns];
		this.heapFileNames = cf.heapFileNames;
		this.columnNames = cf.columnNames;
		size = cf.stringSize * 2 * numberOfColumns + 4 * 2 + 4 * numberOfColumns + cf.stringSize +4;
		this.data = new byte[size];
	}
	/*
	public ColumnarFileMetadata (String name) {

		ColumnarFileMetadata cfm = null;
		try	  {
			Heapfile hf = new Heapfile(name);
			RID rid = new RID();
			Tuple tuple = new Tuple();
			Scan s = hf.openScan();
			tuple = s.getNext(rid);
			cfm = new ColumnarFileMetadata();
			cfm.getColumnarFileMetadata(tuple);
		}
		catch (Exception e)	  {
			e.printStackTrace();
		}   

	}
	 */

	public void getColumnarFileMetadata (Tuple tuple) {

		data = tuple.returnTupleByteArray();
		try {
			this.columnarFileName = Convert.getStrValue(4, data, this.stringSize);
			this.numberOfColumns = Convert.getIntValue(this.stringSize+4, data);
			this.tupleLength = Convert.getIntValue(this.stringSize+4+4, data);

			this.heapFileNames = new String[numberOfColumns];
			this.columnNames = new String[numberOfColumns];
			this.attributeType = new int[numberOfColumns];

			int offset = this.stringSize + (3 * 4);
			for (int i = 0 ; i < this.numberOfColumns ; i++)
			{
				this.attributeType[i] = Convert.getIntValue(offset, data);
				this.columnNames[i] = Convert.getStrValue(offset + 4, data, this.stringSize);
				this.heapFileNames[i] = Convert.getStrValue(offset + 4 + this.stringSize, data, this.stringSize);
				offset += (2* this.stringSize)+4;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void getTuple(){

		try {
			Convert.setIntValue(stringSize, 0, data);
			Convert.setStrValue(this.columnarFileName, 4, data);
			Convert.setIntValue(numberOfColumns, stringSize+4, data);
			Convert.setIntValue(tupleLength, (stringSize+4+4), data);

			int offset = stringSize + (3 * 4);

			for (int i = 0 ; i < numberOfColumns ; i++)	{
				Convert.setIntValue(this.attributeType[i], offset, data);
				Convert.setStrValue(this.columnNames[i], offset+4, data);
				Convert.setStrValue(this.heapFileNames[i], offset+(4+ 25), data);
				offset += (2 * 25) +4;
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Tuple atuple = new Tuple(data, 0, size); 
		//return atuple;
	}

}
