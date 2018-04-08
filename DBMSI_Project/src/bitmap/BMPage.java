/* File BMPage.java */

package bitmap;

import java.io.*;
import java.lang.*;

import btree.ConstructPageException;
import global.*;
import diskmgr.*;
import heap.HFPage;

/**
 * Define constant values for INVALID_SLOT and EMPTY_SLOT
 */


/** Class bitmap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed. 
 */

public class BMPage extends HFPage 
implements GlobalConst{

	//public static final int SIZE_OF_SLOT = 4;
	public static final int DPFIXED =  4 * 2  + 3 * 4;

	public static final int COUNTER = 0;
	//public static final int USED_PTR = 2;
	public static final int FREE_SPACE = 4;
	//public static final int TYPE = 6;
	public static final int PREV_PAGE = 8;
	public static final int NEXT_PAGE = 12;
	public static final int CUR_PAGE = 16;

	public static final int availableMap=(MINIBASE_PAGESIZE-DPFIXED);

	/* Warning:
     These items must all pack tight, (no padding) for
     the current implementation to work properly.
     Be careful when modifying this class.
	 */
	/**
	 * number of slots in use
	 */
	private    short     count; 

	/**
	 * offset of first used byte by data records in data[]
	 */
	//private    short     usedPtr;   

	/**
	 * number of bytes free in data[]
	 */
	private    short     freeSpace;  

	/**
	 * an arbitrary value used by subclasses as needed
	 */
	//private    short     type;     

	/**
	 * backward pointer to data page
	 */
	private    PageId   prevPage = new PageId(); 

	/**
	 * forward pointer to data page
	 */
	private   PageId    nextPage = new PageId();  

	/**
	 *  page number of this page
	 */
	protected    PageId    curPage = new PageId();   

	public int gerStartByte(){
		return DPFIXED;
	}

	public int getAvailableMap (){
		return availableMap;
	} 

	/**
	 * Default constructor
	 */

	public BMPage ()   {  

		try{
			Page apage=new Page();
			PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
			if (pageId==null) 
				throw new ConstructPageException(null, "new page failed");
			this.init(pageId, apage);

		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Constructor of class BMPage
	 * open a B,Page and make this BMpage point to the given page
	 * @param  page  the given page in Page type
	 */
	public BMPage(PageId pid)  { 

		try {
			setCurPage(pid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}  
	public BMPage(Page page) {
		data = page.getpage();
	}
	/**
	 * returns the amount of available space on the page.
	 * @return  the amount of available space on the page
	 * @exception  IOException I/O errors
	 */  
	public int available_space()  
			throws IOException
	{
		freeSpace = Convert.getShortValue (FREE_SPACE, data);
		return (freeSpace);
	}
	public short getCount() 
			throws IOException
	{
		count = Convert.getShortValue (COUNTER, data);
		return (count);
	}
	public void setCount(short counter) 
			throws IOException
	{
		Convert.setShortValue(counter, COUNTER, data);
	}

	/**
	 * Dump contents of a page
	 * @exception IOException I/O errors
	 */
	public void dumpPage()     
			throws IOException
	{
		int i, n ;
		int length, offset;

		curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
		nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
		//usedPtr =  Convert.getShortValue (USED_PTR, data);
		freeSpace =  Convert.getShortValue (FREE_SPACE, data);
		//slotCnt =  Convert.getShortValue (SLOT_CNT, data);

		System.out.println("dumpPage");
		System.out.println("curPage= " + curPage.pid);
		System.out.println("nextPage= " + nextPage.pid);
		//System.out.println("usedPtr= " + usedPtr);
		System.out.println("freeSpace= " + freeSpace);
		//System.out.println("slotCnt= " + slotCnt);

		for(i=0;i<count;i++) {
			//code to traverse through the code
		}


	}

	/**      
	 * Determining if the page is empty
	 * @return true if the HFPage is has no records in it, false otherwise  
	 * @exception  IOException I/O errors
	 */
	public boolean empty() 
			throws IOException
	{
		boolean isEmpty=false;

		// look for an empty slot
		count = Convert.getShortValue (COUNTER, data);

		if (count==0)
			isEmpty= true;

		return isEmpty;
	}

	/**
	 * Constructor of class BMPage
	 * initialise a new page
	 * @param	pageNo	the page number of a new page to be initialised
	 * @param	apage	the Page to be initialised 
	 * @see		Page
	 * @exception IOException I/O errors
	 */


	public void init(PageId pageNo, Page apage)
			throws IOException
	{
		data = apage.getpage();

		count = 0;                // no slots in use
		Convert.setShortValue (count, COUNTER, data);

		curPage.pid = pageNo.pid;
		Convert.setIntValue (curPage.pid, CUR_PAGE, data);

		nextPage.pid = prevPage.pid = INVALID_PAGE;
		Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
		Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);

		freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
		Convert.setShortValue (freeSpace, FREE_SPACE, data);

	}

	/**
	 * Constructor of class BMPage
	 * open a existed BMPage
	 * @param  apage   a page in buffer pool
	 */

	public void openBMpage(Page page)
	{
		data = page.getpage();
	}

	/**
	 * @return 	page number of current page
	 * @exception IOException I/O errors
	 */
	public PageId getCurPage() 
			throws IOException
	{
		curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
		return curPage;
	}

	/**
	 * @return     page number of next page
	 * @exception IOException I/O errors
	 */
	public PageId getNextPage()
			throws IOException
	{
		nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);    
		return nextPage;
	}

	/**
	 * @return	PageId of previous page
	 * @exception IOException I/O errors
	 */
	public PageId getPrevPage()   
			throws IOException 
	{
		prevPage.pid =  Convert.getIntValue (PREV_PAGE, data);
		return prevPage;
	}

	/**
	 * sets value of curPage to pageNo
	 * @param	pageNo	page number for current page
	 * @exception IOException I/O errors
	 */
	public void setCurPage(PageId pageNo)   
			throws IOException
	{
		curPage.pid = pageNo.pid;
		Convert.setIntValue (curPage.pid, CUR_PAGE, data);
	}

	/**
	 * sets value of nextPage to pageNo
	 * @param	pageNo	page number for next page
	 * @exception IOException I/O errors
	 */
	public void setNextPage(PageId pageNo)   
			throws IOException
	{
		nextPage.pid = pageNo.pid;
		Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
	}

	/**
	 * sets value of prevPage to pageNo
	 * @param       pageNo  page number for previous page
	 * @exception IOException I/O errors
	 */
	public void setPrevPage(PageId pageNo)
			throws IOException
	{
		prevPage.pid = pageNo.pid;
		Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
	}

	/**
	 * @return byte array
	 */

	public byte [] getBMpageArray()
	{
		return data;
	}
	public void writeBMpageArray(byte[] x)
	{
		data=x;
	}
	public void setBit(int position, int bit) 
			throws IOException 
	{
		//int positionToUse = position / 8;
		//int setLocation = positionToUse % availableMap;
		//setCount((short) Math.max(setLocation + 1, this.getCount()));
		//positionToUse = position % 8;
		
		data[position+DPFIXED+1] = (byte) (data[position+DPFIXED+1] | bit);
		count++;
		setCount(count);
		/*
		if (bit == 1) 
		{
			short tempAns = (short) (1 << positionToUse);
			data[setLocation + DPFIXED] = (byte) (data[setLocation + DPFIXED] | tempAns);
		} 
		else 
		{
			System.out.println(data[setLocation + DPFIXED]);
			short tempAns = (short) (~(1 << positionToUse));
			data[setLocation + DPFIXED] = (byte) (data[setLocation + DPFIXED] & tempAns);
			System.out.println(data[setLocation + DPFIXED]);
		}*/
	}
}
