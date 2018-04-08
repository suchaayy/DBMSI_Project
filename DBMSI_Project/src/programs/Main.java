package programs;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import bitmap.BM;
import bitmap.BitMapFile;
import btree.BT;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.IntegerKey;
import btree.KeyDataEntry;
import btree.StringKey;
import columnar.ColumnarFile;
import columnar.ColumnarFileMetadata;
import columnar.TupleScan;
import diskmgr.PCounter;
import global.AttrOperator;
import global.AttrType;
import global.Convert;
import global.RID;
import global.SystemDefs;
import global.TID;
import heap.HFPage;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.ColumnarFileScan;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.RelSpec;
import value.IntegerValue;
import value.StringValue;

public class Main {

	public static String filePath = "/Users/sucharitharumesh/eclipse-workspace/DBMSI 2/cse510/";

	public static Scanner scanner = new Scanner(System.in);


	public static void main(String[] args) {

		// Directory of input file

		// Input parameters data file name, column DB name, columnar filename, number of columns
		String dataFileName = args[0];
		String columnDbName = args[1];
		String columnarFileName = args[2];
		int numberOfColumns = Integer.parseInt(args[3]);
		String columnName;
		String indexType;

		ColumnarFile cf = null;
		BTreeFile btf = null; 
		BitMapFile bmf = null;

		try {
			// Declare variable for user's option and defaulting to 0
			int menuOption = 0;
			do {
				// Setting menuOption equal to return value from showMenu();
				menuOption = showMenu();

				// Switching on the value given from user
				switch (menuOption) {

				case 1:
					cf = batchInsert(dataFileName, columnDbName, columnarFileName, numberOfColumns);
					break;
				case 2:

					System.out.println("Enter column name for index: ");
					columnName = scanner.next();

					System.out.println("Enter type of index: ");
					indexType = scanner.next();
					if(indexType.equalsIgnoreCase("BTREE")) {
						btf = indexQueryBTree(cf, columnDbName, columnarFileName, columnName, indexType);
						BT.printBTree(btf.getHeaderPage());
						BT.printAllLeafPages(btf.getHeaderPage());
						System.out.println("Successfully created BTree index !!!");
					} else if(indexType.equalsIgnoreCase("BITMAP")) {
						indexQueryBitmap(cf, columnDbName, columnarFileName, columnName, indexType);
					}
					break;
				case 3:

					String queryInput = null;
					String DBName ="";
					String FileName ="";
					String TargetColumns ="";
					List<String> TargetcolumnNames = new ArrayList<String>();
					List<String> valConstraint = new ArrayList<String>();
					int numBuf = 0;
					String accessType ="";
					System.out.println("\nEnter the query in this format (query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE)\n Enter '-' if you dont give any value for a field");
					BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
					try {
						queryInput = in.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					String[] input = queryInput.split("\\s+");
					DBName = input[1];
					FileName = input[2];
					TargetColumns = input[3];
					TargetColumns = TargetColumns.replaceAll("\\[", "").replaceAll("\\]","");
					String[] colArray = TargetColumns.split(",");
					if(colArray.length > 0 && colArray != null)
					{
						for(String col : colArray)
						{
							TargetcolumnNames.add(col);
						}
					}

					String cName = input[4];
					String operator = input[5];
					String colVal1 = input[6];
					String colVal2 = input[7];

					valConstraint.add(cName);
					valConstraint.add(operator);
					valConstraint.add(colVal1);
					valConstraint.add(colVal2);

					if(input[8].contains("-"))
						numBuf = 0;
					else
						numBuf = Integer.parseInt(input[7]);

					if(input[9].contains("-"))
						accessType = null;
					else
						accessType = input[9];

					System.out.println(accessType);
					System.out.println(TargetcolumnNames);
					runQuery(DBName, cf, FileName, TargetcolumnNames,  valConstraint , numBuf, accessType);

					break;
				case 4:
					String queryInput1 = null;
					String DBName1 ="";
					String FileName1 ="";
					String TargetColumns1 ="";
					List<String> TargetcolumnNames1 = new ArrayList<String>();
					List<String> valConstraint1 = new ArrayList<String>();
					int numBuf1 = 0;
					String accessType1 ="";
					System.out.println("\nEnter the query in this format (query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE)\n Enter '-' if you dont give any value for a field");
					BufferedReader in1 = new BufferedReader(new InputStreamReader(System.in));
					try {
						queryInput = in1.readLine();
					} catch (IOException e) {
						e.printStackTrace();
					}
					String[] input1 = queryInput1.split("\\s+");
					DBName = input1[1];
					FileName = input1[2];
					TargetColumns = input1[3];
					TargetColumns = TargetColumns.replaceAll("\\[", "").replaceAll("\\]","");
					String[] colArray1 = TargetColumns.split(",");
					if(colArray1.length > 0 && colArray1 != null)
					{
						for(String col : colArray1)
						{
							TargetcolumnNames1.add(col);
						}
					}

					String cName1 = input1[4];
					String operator1 = input1[5];
					String colVal11 = input1[6];
					String colVal21 = input1[7];

					valConstraint1.add(cName1);
					valConstraint1.add(operator1);
					valConstraint1.add(colVal11);
					valConstraint1.add(colVal21);

					if(input1[8].contains("-"))
						numBuf = 0;
					else
						numBuf = Integer.parseInt(input1[7]);

					if(input1[9].contains("-"))
						accessType = null;
					else
						accessType = input1[9];

					System.out.println(accessType);
					System.out.println(TargetcolumnNames1);
					deleteQuery(DBName, cf, FileName, TargetcolumnNames1,  valConstraint1 , numBuf, accessType);
					break;
				case 5:
					System.out.println("Quitting Program !!!");
					break;
				default:
					System.out.println("Sorry, please enter valid Option !!!");

				}// End of switch statement

			} while (menuOption != 5);

			// Exiting message when user decides to quit Program
			System.out.println("Thanks for using Columnar Minibase !!!");

		} catch (Exception ex) {
			// Error Message
			ex.printStackTrace();
			System.out.println("Sorry problem occured !!!");
			// flushing scanner
			scanner.next();
		} finally {
			// Ensuring that scanner will always be closed and cleaning up resources
			scanner.close();
		}

	}


	private static void deleteQuery(String columnDBName, ColumnarFile cf, String columnFileName,
		List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType) {


			// [columnName operator value]

			AttrType[] attrTypes = new AttrType[0];
			int columns = 1;

			try {

				switch(accessType) {

				case "BITMAP":
					int colNo = getColumnNumber(valueConstraint.get(0))-1;
					String val = valueConstraint.get(2);
					String file_entry_name = columnFileName+ Integer.toString(colNo)+val;
					BM BitMap = new BM();
					BitMapFile B = new BitMapFile(file_entry_name);
					int[] positions = BM.getpositions(B.getHeaderPage());
					int size = BM.getCount();
					int targetsize = columnNames.size();
					for(int i=0;i<size;i++) { 
						for(int j=0;j<targetsize;j++) {
							int targetColNo = getColumnNumber(columnNames.get(j))-1;
							//System.out.print(HFPage.getvalue_forGivenPosition(positions[i]-20,targetColNo , cf));
							
						}
						System.out.println();
					}

				

				case "BTREE":

					BTreeFile btf = null;

					Heapfile reqHFile=cf.getHeapfileForColumname(columnFileName);
					int columnNumber = cf.getColumnNumberFromColumname(columnFileName);

					String indexColumn = valueConstraint.get(0);
					char operator = valueConstraint.get(1).charAt(0);

					int lowkeyInt=0, hikeyInt=0;
					IntegerKey lowkey=new IntegerKey(0);
					IntegerKey hikey=new IntegerKey(0);
					BTFileScan scan = null;
					KeyDataEntry entry;
					RID rid=new RID();

					try 
					{
						Tuple hTuple=null;
						btf = new BTreeFile("btree"+indexColumn, AttrType.attrInteger, 4, 1);

						BT.printBTree(btf.getHeaderPage());
						System.out.println("printing leaf pages...");

						BT.printAllLeafPages(btf.getHeaderPage());

						lowkeyInt= Integer.parseInt(valueConstraint.get(2));
						lowkey=new IntegerKey(lowkeyInt);
						hikeyInt= Integer.parseInt(valueConstraint.get(3));
						hikey=new IntegerKey(hikeyInt);
						if( lowkeyInt==-3)     
							lowkey=null;
						if( hikeyInt==-2)     
							hikey=null;
						if( hikeyInt!=-1 || lowkeyInt!=-1 ) 
							scan= btf.new_scan(lowkey, hikey);
						while((entry=scan.get_next())!=null)
						{
							if(entry!=null)
							{
								boolean eval=false;
								switch(operator)
								{
								case '>':
									if(entry.key instanceof IntegerKey)
									{
										//System.out.println("asdfgh");
										if(((IntegerKey)entry.key).getKey()> lowkeyInt)
											eval=true;
										else
											eval=false;
									}/*
							else if(entry.key instanceof StringKey)
							{
								if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
									eval=true;
								else
									eval=false;
							}*/
									break;
								case '<':
									if(entry.key instanceof IntegerKey)	{
										if(((IntegerKey)entry.key).getKey() < lowkeyInt)
											eval=true;
										else
											eval=false;
									}/*
							else if(entry.key instanceof StringKey)
							{
								if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
									eval=true;
								else
									eval=false;
							}*/
									break;

								case '=':
									if(entry.key instanceof IntegerKey)
									{
										if(((IntegerKey)entry.key).getKey() == lowkeyInt)
											eval=true;
										else
											eval=false;
									}/*
							else if(entry.key instanceof StringKey)
							{
								if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())==0)
									eval=true;
								else
									eval=false;
							}
							break;*/
								}
								if(eval)
								{
									RID tempRid=new RID();
									System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
									tempRid=((btree.LeafData)entry.data).getData();
									int position=reqHFile.RidToPos(tempRid);
									System.out.println("Found at position: " + position);
									deleteValues(position, cf);
									eval=false;
								}
							}
						}
						System.out.println("AT THE END OF SCAN!");
						//System.out.println("Disk Reads"+ (pcounter.rcounter - startRead));
						//System.out.println("Disk Writes"+ (pcounter.wcounter - startWrite));
					} catch (Exception e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					break;
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	
	

	private static void deleteValues(int position, ColumnarFile cf) {
		
		TID tid = new TID();
		boolean x = cf.markTupleDeleted(tid);
		System.out.println("Successfully Deleted !!!");
	}


	public static int getColumnNumber(String columnName){

		int column = 1;
		switch(columnName){
		case "A":
			column = 1;
			break;

		case "B":
			column = 2;
			break;

		case "C":
			column = 3;
			break;

		case "D":
			column = 4;
			break;
		}
		return column;
	}

	public static int getOp(String op){

		if(op == "=")
			return AttrOperator.aopEQ;
		else if(op == "<")
			return AttrOperator.aopLT;
		else if(op == ">")
			return AttrOperator.aopGT;
		else if(op == "!=")
			return AttrOperator.aopNE;
		else if(op == ">=")
			return AttrOperator.aopGE;
		else
			return AttrOperator.aopLE;
	}

	public static CondExpr[] getValueContraint(List<String> valueContraint){
		if(valueContraint.isEmpty())
			return null;

		int operator = getOp(valueContraint.get(1));
		int column = getColumnNumber(valueContraint.get(0));

		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(operator);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), column);
		expr[0].next = null;

		String value = valueContraint.get(2);
		if (value.matches("\\d*\\.?\\d*")) {
			expr[0].type2 = new AttrType(AttrType.attrReal);
			expr[0].operand2.real = Float.valueOf(value);
		}
		else if(value.matches("\\d+")){
			expr[0].type2 = new AttrType(AttrType.attrInteger);
			expr[0].operand2.integer = Integer.valueOf(value);
		}
		else{
			expr[0].type2 = new AttrType(AttrType.attrString);
			expr[0].operand2.string = value;
		}
		expr[1] = null;
		return expr;
	}


	public static void runQuery(String columnDBName, ColumnarFile cf, String columnFileName,
			List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType) {


		// [columnName operator value]

		AttrType[] attrTypes = new AttrType[0];
		int columns = 1;

		try {

			switch(accessType) {

			case "BITMAP":
				int colNo = getColumnNumber(valueConstraint.get(0))-1;
				String val = valueConstraint.get(2);
				String file_entry_name = columnFileName+ Integer.toString(colNo)+val;
				BM BitMap = new BM();
				BitMapFile B = new BitMapFile(file_entry_name);
				int[] positions = BM.getpositions(B.getHeaderPage());
				int size = BM.getCount();
				int targetsize = columnNames.size();
				for(int i=0;i<size;i++) { 
					//System.out.println("position: "+positions[i]);

					for(int j=0;j<targetsize;j++) {
						int targetColNo = getColumnNumber(columnNames.get(j))-1;
						AttrType x = cf.attributeType[targetColNo];	
						
						if(x.attrType==AttrType.attrInteger)
							System.out.print(HFPage.getIntvalue_forGivenPosition(positions[i],targetColNo , cf));
						else if (x.attrType==AttrType.attrString) 
							System.out.print(HFPage.getStrvalue_forGivenPosition(positions[i],targetColNo , cf));
						System.out.print("  ");
						
					}
					System.out.println();
				}
				System.out.println("End of Bitmap query");
				break;
			case "FILESCAN":
				//query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE	
				int columnNum = getColumnNumber(valueConstraint.get(0))-1;
				String filename = columnFileName + String.valueOf(columnNum);
				AttrType[] types = cf.attributeType;

				AttrType[] attrs = new AttrType[1];
				attrs[0] = new AttrType(types[columnNum].attrType);

				FldSpec[] projlist = new FldSpec[1];
				RelSpec rel = new RelSpec(RelSpec.outer);
				projlist[0] = new FldSpec(rel, 1);

				short[] strsizes = new short[1];
				strsizes[0] = 100;

				CondExpr[] expr = getValueContraint(valueConstraint);

				try {
					FileScan fileScan = new FileScan(filename, attrs, strsizes, (short) 1, 1, projlist, expr);
					Tuple tuple = new Tuple();

					short[] fieldOffset = {0,(short)cf.stringSize,(short)(2*cf.stringSize),(short)(2*cf.stringSize+4)};
					tuple.setTupleMetaData(cf.tupleLength, (short)cf.numberOfColumns, fieldOffset);


					while(true){
						tuple = fileScan.get_next();

						tuple.initHeaders();
						System.out.println(tuple.getIntFld(1));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;



			case "COLUMNSCAN":
				//query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE	
				int columnNum1 = getColumnNumber(valueConstraint.get(0))-1;
				String filename1 = columnFileName + String.valueOf(columnNum1);
				AttrType[] types1 = cf.attributeType;

				AttrType[] attrs1 = new AttrType[1];
				attrs1[0] = new AttrType(types1[columnNum1].attrType);

				FldSpec[] projlist1 = new FldSpec[1];
				RelSpec rel1 = new RelSpec(RelSpec.outer);
				projlist1[0] = new FldSpec(rel1, 1);

				short[] strsizes1 = new short[1];
				strsizes1[0] = 100;

				CondExpr[] expr1 = getValueContraint(valueConstraint);

				try {
					FileScan fileScan = new FileScan(filename1, attrs1, strsizes1, (short) 1, 1, projlist1, expr1);
					Tuple tuple = new Tuple();

					short[] fieldOffset = {0,(short)cf.stringSize,(short)(2*cf.stringSize),(short)(2*cf.stringSize+4)};
					tuple.setTupleMetaData(cf.tupleLength, (short)cf.numberOfColumns, fieldOffset);


					while(true){
						tuple = fileScan.get_next();

						tuple.initHeaders();
						System.out.println(tuple.getIntFld(1));
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				break;

			case "BTREE":

				BTreeFile btf = null;

				Heapfile reqHFile=cf.getHeapfileForColumname(columnFileName);
				int columnNumber = cf.getColumnNumberFromColumname(columnFileName);

				String indexColumn = valueConstraint.get(0);
				char operator = valueConstraint.get(1).charAt(0);

				int lowkeyInt=0, hikeyInt=0;
				IntegerKey lowkey=new IntegerKey(0);
				IntegerKey hikey=new IntegerKey(0);
				BTFileScan scan = null;
				KeyDataEntry entry;
				RID rid=new RID();

				try 
				{
					Tuple hTuple=null;
					btf = new BTreeFile("btree"+indexColumn, AttrType.attrInteger, 4, 1);

					BT.printBTree(btf.getHeaderPage());
					System.out.println("printing leaf pages...");

					BT.printAllLeafPages(btf.getHeaderPage());

					lowkeyInt= Integer.parseInt(valueConstraint.get(2));
					lowkey=new IntegerKey(lowkeyInt);
					hikeyInt= Integer.parseInt(valueConstraint.get(3));
					hikey=new IntegerKey(hikeyInt);
					if( lowkeyInt==-3)     
						lowkey=null;
					if( hikeyInt==-2)     
						hikey=null;
					if( hikeyInt!=-1 || lowkeyInt!=-1 ) 
						scan= btf.new_scan(lowkey, hikey);
					while((entry=scan.get_next())!=null)
					{
						if(entry!=null)
						{
							boolean eval=false;
							switch(operator)
							{
							case '>':
								if(entry.key instanceof IntegerKey)
								{
									//System.out.println("asdfgh");
									if(((IntegerKey)entry.key).getKey()> lowkeyInt)
										eval=true;
									else
										eval=false;
								}/*
								else if(entry.key instanceof StringKey)
								{
									if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
										eval=true;
									else
										eval=false;
								}*/
								break;
							case '<':
								if(entry.key instanceof IntegerKey)	{
									if(((IntegerKey)entry.key).getKey() < lowkeyInt)
										eval=true;
									else
										eval=false;
								}/*
								else if(entry.key instanceof StringKey)
								{
									if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())>0)
										eval=true;
									else
										eval=false;
								}*/
								break;

							case '=':
								if(entry.key instanceof IntegerKey)
								{
									if(((IntegerKey)entry.key).getKey() == lowkeyInt)
										eval=true;
									else
										eval=false;
								}/*
								else if(entry.key instanceof StringKey)
								{
									if(((StringKey)entry.key).getKey().compareTo(((bitmap.StringKey)value).getKey())==0)
										eval=true;
									else
										eval=false;
								}
								break;*/
							}
							if(eval)
							{
								RID tempRid=new RID();
								System.out.println("SCAN RESULT: "+ entry.key + " " + entry.data);
								//tempRid=((btree.LeafData)entry.data).getData();
								//int position=reqHFile.RidToPos(tempRid);
								//System.out.println("Found at position: " + position);
								//getProjection(position, columnNames1,);
								eval=false;
							}
						}
					}
					System.out.println("AT THE END OF SCAN!");
					//System.out.println("Disk Reads"+ (pcounter.rcounter - startRead));
					//System.out.println("Disk Writes"+ (pcounter.wcounter - startWrite));
				} catch (Exception e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				break;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static int showMenu() {

		int option = 0;

		// Printing menu to screen
		System.out.println("CSE 510: Columnar Minibase");
		System.out.println("-------------------------------------------");
		System.out.println("Menu:");
		System.out.println("1. Batch Insert Program");
		System.out.println("2. Index Program");
		System.out.println("3. Query Program");
		System.out.println("4. Delete Query Program");
		System.out.println("5. Quit Program");
		System.out.println();
		// Getting user option from above menu
		System.out.print("Enter Option from above: ");
		option = scanner.nextInt();
		return option;

	}// End of showMenu



	public static void indexQueryBitmap(ColumnarFile cf, String columnDbName, 
			String columnarFileName, String columnName, String indexType) {

		BitMapFile B = null; 

		String[] columnNames;
		String file_entry_name;

		ColumnarFileMetadata cfm = cf.getColumnarFileMetadata(columnarFileName+".hdr");
		columnNames = cfm.columnNames;
		cf.columnNames = columnNames;

		int startread, startwrite;
		startread = PCounter.rcounter;
		startwrite = PCounter.wcounter;

		Heapfile hf = cf.getHeapfileForColumname(columnName);

		if(indexType.equalsIgnoreCase("BITMAP")) {
			try {
				int value;
				String str;
				RID rid = new RID();
				Scan s = hf.openScan();
				Tuple tuple=null;
				System.out.println("Creating BitMap on column: "+columnName);
				int ColNo = cf.getColumnNumberFromColumname(columnName);
				AttrType x = cf.attributeType[ColNo];	

				HashSet<Integer> intHash = new HashSet<>();
				HashSet<String> strHash = new HashSet<>();

				while((tuple=s.getNext(rid))!=null){

					if(x.attrType == AttrType.attrInteger) {
						value = Convert.getIntValue(0, tuple.getData());

						if (!intHash.contains(value)) {
							IntegerValue val = new IntegerValue();
							val.setValue(value);
							intHash.add(value);
							file_entry_name= columnarFileName+Integer.toString(ColNo)+Integer.toString(value);
							//System.out.println(file_entry_name);
							System.out.println("Creating index on: " + value);
							B = new BitMapFile(file_entry_name,cf,ColNo,val);
							//BM.printBitMap(B.getHeaderPage());
						}

					}	
					else if(x.attrType == AttrType.attrString)	{
						str = Convert.getStrValue(0, tuple.getData(),cf.stringSize );					 

						if (!strHash.contains(str)) {
							StringValue vals = new StringValue();
							vals.setValue(str);
							strHash.add(str);
							file_entry_name= columnarFileName+Integer.toString(ColNo)+str;
							//System.out.println(file_entry_name);
							System.out.println("Creating index on: " + str);
							B = new BitMapFile(file_entry_name,cf,ColNo,vals);
							//BM.printBitMap(B.getHeaderPage());
						}
					}

				}
			} catch (Exception e) {
				// TODO: handle exception
			}

			System.out.println("Number of disk reads: " + (PCounter.rcounter-startread));
			System.out.println("Number of disk writes: " + (PCounter.wcounter-startwrite));
		}

	}

	public static BTreeFile indexQueryBTree(ColumnarFile cf, String columnDbName, 
			String columnarFileName, String columnName, String indexType) {

		BTreeFile bTreeFile = null; 

		int startread = 0, startwrite = 0;

		String[] columnNames;

		ColumnarFileMetadata cfm = cf.getColumnarFileMetadata(columnarFileName+".hdr");
		columnNames = cfm.columnNames;
		cf.columnNames = columnNames;

		Heapfile hf = cf.getHeapfileForColumname(columnName);

		if(indexType.equalsIgnoreCase("BTREE")) {
			try {

				startread = PCounter.rcounter;
				startwrite = PCounter.wcounter;

				RID rid = new RID();
				Scan s = hf.openScan();
				Tuple tuple=null;
				System.out.println("Creating BTree on column: "+columnName);

				bTreeFile = new BTreeFile("btree"+columnName, AttrType.attrInteger, 4, 1);

				while((tuple=s.getNext(rid))!=null){
					int temp=Convert.getIntValue(0, tuple.getData());
					bTreeFile.insert(new btree.IntegerKey(temp), rid);
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		System.out.println("Number of disk reads: " + (PCounter.rcounter-startread));
		System.out.println("Number of disk writes: " + (PCounter.wcounter-startwrite));

		return bTreeFile;
	}



	public static ColumnarFile batchInsert(String dataFileName, String columnDbName, 
			String columnarFileName, int numberOfColumn) {


		ColumnarFile cf = null;
		// Parameters initialized
		String[] columnNames = new String[numberOfColumn];
		AttrType[] columnTypes = new AttrType[numberOfColumn];

		int i = 0;
		int tupleLength = 0;
		int stringSize = 0;
		int startread, startwrite;

		@SuppressWarnings("unused")
		SystemDefs sysDef = new SystemDefs(columnDbName,100000,100,"Clock");

		FileInputStream fin;
		try {
			fin = new FileInputStream(filePath+dataFileName);
			DataInputStream din = new DataInputStream(fin);
			BufferedReader bin = new BufferedReader(new InputStreamReader(din));

			startread = PCounter.rcounter;
			startwrite = PCounter.wcounter;

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

			cf = new ColumnarFile (columnarFileName, numberOfColumn, columnTypes, stringSize);
			cf.columnNames = columnNames;
			cf.setColumnarFileMetadata(stringSize);

			byte [] tupleData = new byte[tupleLength];
			int offset = 0;
			int count = 1;

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
				System.out.println("Record Inserted: "+ count);
				offset = 0;
				count++;
				Arrays.fill(tupleData, (byte)0);

			}
			System.out.println();
			System.out.println("Successfully inserted all the records !!");		
			System.out.println("Number of disk reads: " + (PCounter.rcounter-startread));
			System.out.println("Number of disk writes: " + (PCounter.wcounter-startwrite));

			System.out.println();

			TupleScan tscan = new TupleScan(cf);
			TID tid = new TID();
			tid.recordIDs = new RID[numberOfColumn];

			Tuple tuple = new Tuple();
			short[] fieldOffset = {0,(short)cf.stringSize,(short)(2*cf.stringSize),(short)(2*cf.stringSize+4)};
			tuple.setTupleMetaData(tupleLength, (short)numberOfColumn, fieldOffset);

			for (i = 0; i < numberOfColumn; i++) {
				tid.recordIDs[i] = new RID();
			}

			while((tuple = tscan.getNext(tid)) != null) {
				System.out.print("Record Fetched: ");
				tuple.print(columnTypes);
			}
			System.out.println();
			System.out.println("Successfully fetched all the records !!");

			System.out.println("Number of disk reads: " + (PCounter.rcounter-startread));
			System.out.println("Number of disk writes: " + (PCounter.wcounter-startwrite));

			tscan.closetuplescan();	
			bin.close();




		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return cf;

	}


}
