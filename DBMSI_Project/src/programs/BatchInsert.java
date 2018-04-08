package programs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

import columnar.ColumnarFile;
import columnar.TupleScan;
import diskmgr.PCounter;
import global.AttrType;
import global.Convert;
import global.RID;
import global.SystemDefs;
import global.TID;
import heap.Tuple;

public class BatchInsert {

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
		
		@SuppressWarnings("unused")
		SystemDefs sysDef = new SystemDefs(args[1],100000,100,"Clock");

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
			System.out.println(cf.cfm.columnarFileName);

			for (int j = 0; j < noOfColumns; j++) {
				System.out.println(cf.columnNames[j]);
			}

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
				System.out.println("Record Inserted");
				offset = 0;

				Arrays.fill(tupleData, (byte)0);
				
			}
			System.out.println();
			System.out.println("Successfully inserted all the records !!");
			System.out.println();
			
			TupleScan tscan = new TupleScan(cf);
			TID tid = new TID();
			tid.recordIDs = new RID[Integer.parseInt(args[3])];
			
			Tuple tuple = new Tuple();
			short[] fieldOffset = {0,(short)cf.stringSize,(short)(2*cf.stringSize),(short)(2*cf.stringSize+4)};
			tuple.setTupleMetaData(tupleLength, (short)Integer.parseInt(args[3]), fieldOffset);
			
			for (i=0; i<Integer.parseInt(args[3]); i++) {
				tid.recordIDs[i] = new RID();
			}
			
			while((tuple = tscan.getNext(tid)) != null) {
				System.out.print("Record Fetched: ");
				tuple.print(columnTypes);
			}
			System.out.println();
			System.out.println("Successfully fetched all the records !!");
			tscan.closetuplescan();	
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
