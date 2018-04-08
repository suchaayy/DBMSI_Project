package bitmap;


import java.io.*;
import btree.*;
import global.*;
import diskmgr.*;
import bufmgr.*;
import columnar.*;
import value.*;
import heap.*;

public class BitMapFile implements GlobalConst {
	private final static int MAGIC0=1989;

	private final static String lineSep=System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	public static void traceFilename(String filename) 
			throws  IOException
	{

		fos=new FileOutputStream(filename);
		trace=new DataOutputStream(fos);
	}
	public static void destroyTrace() 
			throws  IOException
	{
		if( trace != null) trace.close();
		if( fos != null ) fos.close();
		fos=null;
		trace=null;
	}

	private BitMapHeaderPage headerPage;
	private  PageId  headerPageId;
	private String  FileName;  

	public BitMapHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String fileEntryname)         
			throws GetFileEntryException
	{
		try {
			return SystemDefs.JavabaseDB.get_file_entry(fileEntryname);
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e,"");
		}
	}
	private void add_file_entry(String fileEntryName,  PageId start_page_num) 
			throws AddFileEntryException
	{
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileEntryName,  start_page_num);//filename and its PGID is added to DB.
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e,"");
		}      
	}


	private Page pinPage(PageId pageno) 
			throws PinPageException
	{
		try {
			Page page=new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
			return page;// return the pg if not pinned.
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e,"");
		}
	}

	private void unpinPage(PageId pageno) 
			throws UnpinPageException
	{ 
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		} 
	}

	private void freePage(PageId pageno) 
			throws FreePageException
	{
		try{
			SystemDefs.JavabaseBM.freePage(pageno);    
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e,"");
		} 

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException
	{
		try {	
			SystemDefs.JavabaseDB.delete_file_entry( filename );
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e,"");
		} 
	}

	private void unpinPage(PageId pageno, boolean dirty) 
			throws UnpinPageException
	{
		try{
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);  
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e,"");
		}  
	}
	//Constructor when file already exist;
	public BitMapFile (String file_entry_name)
			throws GetFileEntryException,
			PinPageException,
			ConstructPageException
	{
		headerPageId=get_file_entry(file_entry_name);

		headerPage=new BitMapHeaderPage(headerPageId);
		FileName = new String(file_entry_name);

	}

	//Constructor to create a new file, if it doesnt exist; 
	public BitMapFile( String file_entry_name, ColumnarFile  columnfile, int columno, ValueClass value)
			throws GetFileEntryException, 
			ConstructPageException,
			IOException, 
			AddFileEntryException, InvalidTupleSizeException, FieldNumberOutOfBoundException {

		//headerPageId=get_file_entry(filename, columnfile);

		if( headerPageId==null) //file not exist
		{
			headerPage= new  BitMapHeaderPage(); 
			headerPageId= headerPage.getPageId();
			add_file_entry(file_entry_name, headerPageId);
			
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_ColNo(columno);
			headerPage.set_value(value);
			headerPage.setType(NodeType.BTHEAD);
		}
		else {
			headerPage = new BitMapHeaderPage(headerPageId);  
		}

		FileName=new String(file_entry_name);
		
		if(value instanceof IntegerValue)
		{ int intkeyval = ((IntegerValue)value).getValue();
		accessInt(columnfile,columno,intkeyval);
		}
		else if(value instanceof StringValue) {
			String strkeyval = ((StringValue)value).getValue();
			accessStr(columnfile,columno,strkeyval);}



	}

	public void accessStr(ColumnarFile columnfile, int columno, String Value) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {

		Tuple t = new Tuple();
		//short[] fieldOffset = {0,(short)columnfile.stringSize,(short)(2*columnfile.stringSize),(short)(2*columnfile.stringSize+4)};
		//t.setTupleMetaData(columnfile.tupleLength, (short)columnfile.numberOfColumns, fieldOffset);
		
		int position = 0;

		RID rid = new RID();
		Scan columnScan = columnfile.openColumnScan(columno);


		while ((t = columnScan.getNext(rid)) != null) {
			short[] fieldOffset = {0,(short)columnfile.stringSize,(short)(2*columnfile.stringSize),(short)(2*columnfile.stringSize+4)};
			t.setTupleMetaData(columnfile.tupleLength, (short)columnfile.numberOfColumns, fieldOffset);
			
			String colVal = t.getStrFld(1);
			if(colVal.equals(Value)) {
				insert(position);
			} else {
				delete(position);
			}
			position++;
		}

	}
	
	public void accessInt(ColumnarFile columnfile, int columno, int Value) throws InvalidTupleSizeException, IOException, FieldNumberOutOfBoundException {

		Tuple t = new Tuple();
		int position = 0;

		RID rid = new RID();
		Scan columnScan = columnfile.openColumnScan(columno);


		while ((t = columnScan.getNext(rid)) != null) {
			short[] fieldOffset = {0,(short)columnfile.stringSize,(short)(2*columnfile.stringSize),(short)(2*columnfile.stringSize+4)};
			t.setTupleMetaData(columnfile.tupleLength, (short)columnfile.numberOfColumns, fieldOffset);
			
			int colVal = t.getIntFld(1);
			if(colVal == Value) {
				insert(position);
			} else {
				delete(position);
			}
			position++;
		}

	}


	public void close()
			throws PageUnpinnedException, 
			InvalidFrameNumberException, 
			HashEntryNotFoundException,
			ReplacerException
	{
		if ( headerPage!=null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);//wtf??
			headerPage=null;
		}  
	}

	public boolean insert (int position)
			throws IOException {
		//BMPage page = new BMPage();
		PageId apage = new PageId();

		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			BMPage page = new BMPage();
			page.setNextPage(new PageId(INVALID_PAGE));
			headerPage.set_rootId(page.getCurPage());
			//System.out.println("root set to: "+headerPage.get_rootId().pid);
			page.setBit(position,1);
			return true;

		}

		if(headerPage.get_rootId().pid != INVALID_PAGE) {
			PageId p = headerPage.get_rootId(); 
			//apage = headerPage.set_rootId(pid);
			Page pg1 = null;
			try {
				pg1 = pinPage(p);
			} catch (PinPageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			BMPage page = new BMPage(pg1);
			page.setNextPage(new PageId(INVALID_PAGE));
			//page.openBMpage(p);
			
			//byte [] data;
			if(page.available_space() != 0) {
				//data = page.getBMpageArray();
				int count = page.getCount();
				
				//System.out.println(count);
				
				if(position > count+1)
					return false;
				else {
					page.setBit(position, 1);
					try {
						unpinPage(p);
					} catch (UnpinPageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return true;
				}
			}
			else {
				BMPage page1= new BMPage();
				PageId  apage1 = new PageId();
				page.setNextPage(apage1);
				page1.setCurPage(apage1);
				page1.setPrevPage(apage);
				Page pg2 = null;
				try {
					pg2 = pinPage(apage1);
				} catch (PinPageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				BMPage page2 = new BMPage(pg2);
				//page2.openBMpage(apage1);
				page1.setNextPage(new PageId(INVALID_PAGE));
				//byte [] data2;
				if(page2.available_space() != 0) {
					//data2 = page2.getBMpageArray();
					int count2 = page2.getCount();
					if(position > count2+1)
						return false;
					else {
						page2.setBit(position, 1);
						try {
							unpinPage(apage1);
						} catch (UnpinPageException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return true;
					}

				}

			}
		}
		return true;		 

	}

	public boolean delete (int position)
			throws IOException {

		PageId apage = new PageId();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			BMPage page = new BMPage();
			headerPage.set_rootId(page.getCurPage());
			//System.out.println("root set to: "+headerPage.get_rootId().pid);
			page.setNextPage(new PageId(INVALID_PAGE));
			if(page.available_space() == 0) 
			{
				page.setBit(position,0);
				return true;
			}
		}
		
		if(headerPage.get_rootId().pid != INVALID_PAGE) {
			PageId p = headerPage.get_rootId(); 
			//apage = headerPage.set_rootId(pid);
			Page pg1 = null;
			try {
				pg1 = pinPage(p);
			} catch (PinPageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			BMPage page = new BMPage(pg1);
			page.setNextPage(new PageId(INVALID_PAGE));
			//page.openBMpage(p);
			
			//byte [] data;
			if(page.available_space() != 0) {
				//data = page.getBMpageArray();
				int count = page.getCount();
				
				//System.out.println(count);
				
				if(position > count+1)
					return false;
				else {
					page.setBit(position, 0);
					try {
						unpinPage(p);
					} catch (UnpinPageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return true;
				}
			}
			else {
				BMPage page1= new BMPage();
				PageId  apage1 = new PageId();
				page.setNextPage(apage1);
				page1.setCurPage(apage1);
				page1.setPrevPage(apage);
				Page pg2 = null;
				try {
					pg2 = pinPage(apage1);
				} catch (PinPageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				BMPage page2 = new BMPage(pg2);
				//page2.openBMpage(apage1);
				page1.setNextPage(new PageId(INVALID_PAGE));
				//byte [] data2;
				if(page2.available_space() != 0) {
					//data2 = page2.getBMpageArray();
					int count2 = page2.getCount();
					if(position > count2+1)
						return false;
					else {
						page2.setBit(position, 0);
						try {
							unpinPage(apage1);
						} catch (UnpinPageException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return true;
					}

				}

			}
		}
		return true;		 


	}
	
	public void destroyFile()
			throws IOException, 
			   IteratorException, 
			   UnpinPageException,
			   FreePageException,   
			   DeleteFileEntryException, 
			   ConstructPageException,
			   PinPageException 
			   {
					if( headerPage != null) {
						PageId pgID = headerPage.get_rootId();
						BMPage page = new BMPage (pgID);
						page.dumpPage();
						unpinPage(headerPageId);
						freePage(headerPageId);
						delete_file_entry(FileName);
						headerPage = null;
					}
			   }

}
