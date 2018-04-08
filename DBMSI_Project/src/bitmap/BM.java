/*
 * @(#) BT.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package bitmap;

import java.io.*;
import java.lang.*;

import btree.ConstructPageException;
import btree.IteratorException;
import btree.PinPageException;
import btree.UnpinPageException;
import global.*;
import diskmgr.*;
import bufmgr.*;
import heap.*;

/**  
 * This file contains, among some debug utilities, the interface to our
 * key and data abstraction.  The BTLeafPage and BTIndexPage code
 * know very little about the various key types.  
 *
 * Essentially we provide a way for those classes to package and 
 * unpackage <key,data> pairs in a space-efficient manner.  That is, the 
 * packaged result contains the key followed immediately by the data value; 
 * No padding bytes are used (not even for alignment). 
 *
 * Furthermore, the BT<*>Page classes need
 * not know anything about the possible AttrType values, since all 
 * their processing of <key,data> pairs is done by the functions 
 * declared here.
 *
 * In addition to packaging/unpacking of <key,value> pairs, we 
 * provide a keyCompare function for the (obvious) purpose of
 * comparing keys of arbitrary type (as long as they are defined 
 * here).
 *
 * For debug utilities, we provide some methods to print any page
 * or the whole B+ tree structure or all leaf pages.
 *
 */
public class BM  implements GlobalConst{
  //Default Constructor
    static int[] positions ;
    static int c;
    public BM() {
        positions = new int[1000];
    c=0;
        
    }
    public static Page pinPage(PageId pageno) 
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
    public static void unpinPage(PageId pageno) 
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
    /** For debug. Print Bitmap structure out
       *@param header  the head page of the Bitmapfile
       *@exception IOException error from the lower layer
       *@exception ConstructPageException  error from BT page constructor
       *@exception IteratorException  error from iterator
       *@exception HashEntryNotFoundException  error from lower layer
       *@exception InvalidFrameNumberException  error from lower layer
       *@exception PageUnpinnedException  error from lower layer
       *@exception ReplacerException  error from lower layer
       */
      public static void printBitMap(BitMapHeaderPage header) 
        throws IOException, 
           ConstructPageException, 
           IteratorException,
           HashEntryNotFoundException,
           InvalidFrameNumberException,
           PageUnpinnedException,
           ReplacerException 
        {  System.out.println(header.get_rootId().pid);
          if(header.get_rootId().pid == INVALID_PAGE) {
        System.out.println("The BitMap is Empty!!!");
        return;
          }
          
          System.out.println("");
          System.out.println("");
          System.out.println("");
          System.out.println("---------------BitMap---------------");
          
          
          System.out.println(1+ "     "+header.get_rootId());
          
          //System.out.println(header.get_ColNo());
          
          _printPage(header.get_rootId());
          
          System.out.println("--------------- End ---------------");
          System.out.println("");
          System.out.println("");
        }

      private static void _printPage(PageId currentPageId) 
        throws IOException 
      {

            //BMPage page = new BMPage(currentPageId);
            //PageId pageid = page.getCurPage();
            //page.openBMpage(page);
            Page pg1 = null;
            try {
                pg1 = pinPage(currentPageId);
            } catch (PinPageException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            BMPage page = new BMPage(pg1);
            byte [] data;
            data = page.getBMpageArray();
            
            int count = page.getCount();
            int start = page.gerStartByte()+1;
            int end = start +count;
            for(int i = start; i < end; i++){
                System.out.println( data[i]);
            }
            
            PageId apage = page.getNextPage();
             try {
                unpinPage(currentPageId);
            } catch (UnpinPageException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            if(apage.pid != INVALID_PAGE) {
            _printPage(apage);
            }
            }
            public static int getCount() {
                return c;
            }
           public static int[] getpositions(BitMapHeaderPage header) throws IOException {
                
                if(header.get_rootId().pid == INVALID_PAGE) {
                    System.out.println("The BitMap is Empty!!!");
                    return null;
                  }
                else {
                    getpos(header.get_rootId());
                  }
                return positions;
            }
            
         private static void getpos( PageId currentPageId) throws IOException {
                
                
                    Page pg1 = null;
                try {
                    pg1 = pinPage(currentPageId);
                } catch (PinPageException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                BMPage page = new BMPage(pg1);
                byte [] data;
                data = page.getBMpageArray();
                
                int count = page.getCount();
                int start = page.gerStartByte()+1;
                int end = start +count;
                for(int i = start; i < end; i++){
                        if(data[i]==1)
                        { positions[c]=i;
                          c++;
                        }
                }
                PageId apage = page.getNextPage();
                 try {
                    unpinPage(currentPageId);
                } catch (UnpinPageException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if(apage.pid != INVALID_PAGE) {
                getpos(apage);
                }
                
                
            }
      
      

  } // end of BM






