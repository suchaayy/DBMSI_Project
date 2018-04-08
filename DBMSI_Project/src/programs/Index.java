package programs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

import btree.BT;
import btree.BTreeFile;
import columnar.ColumnarFile;
import columnar.ColumnarFileMetadata;
import global.AttrType;
import global.Convert;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class Index {
	
	public static void main(String[] args) {

		String filePath = "C:/Users/Ashish/ASU Backup/ASU Grad School/Semester 2 (Spring 2018)/Courses/DBMSI/Phase_2/";
		String dataFileName = args[0];

		int i = 0;
		int tupleLength = 0;
		int stringSize = 0;

		String[] columnNames = new String[Integer.parseInt(args[3])];
		AttrType[] columnTypes = new AttrType[Integer.parseInt(args[3])];

		String columnarFileName = args[2];
		int noOfColumns = Integer.parseInt(args[3]);
		String columnName = args[4];
		String indexType = args[5];

		BTreeFile bTreeFile = null; 

		@SuppressWarnings("unused")
		SystemDefs sysdef = new SystemDefs(args[1],100000,100,"Clock");

		FileInputStream fin;
		try {
			fin = new FileInputStream(filePath+dataFileName);
			DataInputStream din = new DataInputStream(fin);
			BufferedReader bin = new BufferedReader(new InputStreamReader(din));

			String line = bin.readLine();

			StringTokenizer st = new StringTokenizer(line);

			while(st.hasMoreTokens()) {

				String token = st.nextToken();
				StringTokenizer svalue = new StringTokenizer(token);

				String value1 = svalue.nextToken(":");
				String value2 = svalue.nextToken(":");

				//System.out.println(value1 + " " + value2);

				columnNames[i] = value1;

				if (value2.equals("int")) {
					columnTypes[i] = new AttrType(AttrType.attrInteger);
					tupleLength = tupleLength + 4;
				} else {
					columnTypes[i] = new AttrType(AttrType.attrString);				
					StringTokenizer t1 = new StringTokenizer(value2);

					t1.nextToken("(");
					String temp = t1.nextToken("(");
					t1 = new StringTokenizer(temp);
					temp = t1.nextToken(")");
					stringSize = Integer.parseInt(temp);
					tupleLength = tupleLength + stringSize;
				}
				i++;
			}

			ColumnarFile cf = new ColumnarFile (columnarFileName, noOfColumns, columnTypes, stringSize);
			cf.columnNames = columnNames;
			cf.setColumnarFileMetadata(25);

			byte [] tupleData = new byte[tupleLength];
			int offset = 0;
			while((line = bin.readLine()) != null) {

				StringTokenizer columnValue = new StringTokenizer (line);

				for(AttrType columnType: columnTypes){
					String column = columnValue.nextToken();
					if(columnType.attrType == AttrType.attrInteger) {
						Convert.setIntValue(Integer.parseInt(column), offset, tupleData);
						offset = offset + 4;
					}
					else if (columnType.attrType == AttrType.attrString){
						Convert.setStrValue(column, offset, tupleData);
						offset = offset + stringSize;
					}
				}	
				cf.insertTuple(tupleData);
				offset = 0;

				Arrays.fill(tupleData, (byte)0);

			}

			ColumnarFileMetadata cfm = cf.getColumnarFileMetadata(columnarFileName+".hdr");
			columnNames = cfm.columnNames;
			cf.columnNames = columnNames;

			Heapfile hf = cf.getHeapfileForColumname(columnName);

			if(indexType.equalsIgnoreCase("BTREE")) {
				try {
					RID rid = new RID();
					Scan s = hf.openScan();
					Tuple tuple=null;
					System.out.println("Creating BTree on column: "+columnName);

					bTreeFile = new BTreeFile("btree"+columnName, AttrType.attrInteger, 4, 1);

					while((tuple=s.getNext(rid))!=null){
						int temp=Convert.getIntValue(0, tuple.getData());
						bTreeFile.insert(new btree.IntegerKey(temp), rid);
					}

					BT.printBTree(bTreeFile.getHeaderPage());
					BT.printAllLeafPages(bTreeFile.getHeaderPage());

					System.out.println("Successfully created BTree index !!!");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			bin.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
}