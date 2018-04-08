package bitmap;

import java.io.IOException;

import btree.ConstructPageException;
import value.StringValue;
import value.ValueClass;
import diskmgr.Page;
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

/**
 * Interface of a BitMap index header page.  
 * Here we use a HFPage as head page of the file
 * Inside the headerpage, Logically, there are only seven
 * elements inside the head page, they are
 * magic0, rootId,colNo,value 
 * 
 * and type(=NodeType.BTHEAD)
 */

public class BitMapHeaderPage extends HFPage{
	static int len = 0;
	void setPageId(PageId pageno) 
			throws IOException 
	{
		setCurPage(pageno);
	}

	PageId getPageId()
			throws IOException
	{
		return getCurPage();
	}
	/** set the magic0
	 *@param magic  magic0 will be set to be equal to magic  
	 */
	void set_magic0( int magic ) 
			throws IOException 
	{
		setPrevPage(new PageId(magic)); 
	}


	/** get the magic0
	 */
	int get_magic0()
			throws IOException 
	{ 
		return getPrevPage().pid;
	};

	/** set the rootId
	 */
	void  set_rootId( PageId rootID )
			throws IOException 
	{
		setNextPage(rootID); 
	};

	/** get the rootId
	 */
	PageId get_rootId()
			throws IOException
	{ 
		return getNextPage();
	};

	/** get the colNo
	 */
	void set_ColNo(int colNo ) 
			throws IOException
	{
		setSlot(1, colNo, 0); 
	}

	/** set the colNo
	 */ 
	int get_ColNo() 
			throws IOException
	{
		return getSlotLength(1);
	}    
	/** get the value
	 */
	void set_value(ValueClass value ) 
			throws IOException
	{
		setSlotvalue(2, value, 0); 
		if(value instanceof StringValue) {
			len = (((StringValue)value).getValue()).length()+2;
		}
	}

	/** set the valuesize
	 */ 
	int get_integervalue() 
			throws IOException
	{   return get_intvalue(2);
	//return getSlotLengthStr(2);
	}
	String get_stringvalue() 
			throws IOException
	{

		return get_strvalue(2,len);
	}

	/** pin the page with pageno, and get the corresponding SortedPage
	 */
	public BitMapHeaderPage(PageId pageno) 
			throws ConstructPageException
	{ 
		super();
		try {

			SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/); 
		}
		catch (Exception e) {
			throw new ConstructPageException(e, "pinpage failed");
		}
	}

	/**associate the SortedPage instance with the Page instance */
	public BitMapHeaderPage(Page page) {

		super(page);
	}  


	/**new a page, and associate the SortedPage instance with the Page instance
	 */
	public BitMapHeaderPage( ) 
			throws ConstructPageException
	{
		super();
		try{
			Page apage=new Page();
			PageId pageId=SystemDefs.JavabaseBM.newPage(apage,1);
			if (pageId==null) 
				throw new ConstructPageException(null, "new page failed");
			this.init(pageId, apage);

		}
		catch (Exception e) {
			throw new ConstructPageException(e, "construct header page failed");
		}
	}  

}
