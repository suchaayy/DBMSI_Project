package tests;

import columnar.ColumnarFile;
import global.AttrOperator;
import global.AttrType;
import global.GlobalConst;
import global.SystemDefs;
import heap.Tuple;
import iterator.ColumnarFileScan;
import java.io.BufferedReader;

import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.io.IOException;
import java.lang.Exception;
import java.util.ArrayList;
import java.util.List;
import java.io.*;

//Define the Sample Data schema

class QyeryDriver extends TestDriver implements GlobalConst {


    public QyeryDriver() {
        super("query");
    }

    public boolean runTests() {
        System.out.print("\n" + "Running " + testName() +  " test...." + "\n");

        try {
            SystemDefs sysdef = new SystemDefs(dbpath, 8193, 100, "Clock");

        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here.  If we need to port this
        // program to other platform, the remove_cmd have to be
        // modified accordingly.
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);

        } catch (IOException e) {
            System.err.println("" + e);
        }

        System.out.print("\n" + "..." + testName() + " tests ");
        System.out.print(_pass == OK ? "completely successfully" : "failed");
        System.out.print(".\n\n");

        return _pass;
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


    public static void runQuery(String columnDBName, String columnFileName,
                                List<String> columnNames, List<String> valueConstraint, int numBuf, String accessType) {


        AttrType[] attrTypes = new AttrType[0];
        int columns = 1;
        try {


            switch(accessType)
            {
                case "COLUMNSCAN":
                    //query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE
                    int columnNum = getColumnNumber(valueConstraint.get(0));
                    String filename = columnFileName + '.' + String.valueOf(columnNum);
                    ColumnarFile columnarFile = new ColumnarFile(filename); // Constructor which returns the object of the columnarfile that already exists..!
                    AttrType[] types = columnarFile.attributeType;

                    AttrType[] attrs = new AttrType[1];
                    attrs[0] = new AttrType(types[columnNum].attrType);

                    FldSpec[] projlist = new FldSpec[1];
                    RelSpec rel = new RelSpec(RelSpec.outer);
                    projlist[0] = new FldSpec(rel, 1);

                    short[] strsizes = new short[1];
                    strsizes[0] = 100;

                    CondExpr[] expr = getValueContraint(valueConstraint);

                    try {
                        ColumnarFileScan columnarFileScan = new ColumnarFileScan(filename, attrs, strsizes, (short) 1, 1, projlist, expr);
                        Tuple tuple;
                        while(true){
                            tuple = columnarFileScan.get_next();
                            if(tuple == null) break;
                            tuple.initHeaders();
                            System.out.println(tuple.getIntFld(1));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    protected boolean test1()
    {
        /** delete leaf entry  given its <key, rid> pair.
         *  `rid' is IN the data entry; it is not the id of the data entry)
         *@param key the key in pair <key, rid>. Input Parameter.
         *@param rid the rid in pair <key, rid>. Input Parameter.
         *@return true if deleted. false if no such record.
         *
         */
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
        String colVal = input[6];

        valConstraint.add(cName);
        valConstraint.add(operator);
        valConstraint.add(colVal);

        if(input[7].contains("-"))
            numBuf = 0;
        else
            Integer.parseInt(input[7]);

        if(input[8].contains("-"))
            accessType = null;
        else
            accessType = input[8];
        runQuery(DBName, FileName, TargetcolumnNames,  valConstraint , numBuf, accessType);

        return true;
    }
    protected boolean runAllTests() {
        boolean _passAll = OK;

        if (!test1()) { _passAll = FAIL; }

        try{
            SystemDefs.JavabaseDB.DBDestroy();

        }
        catch (IOException e){
            System.err.println(" DB already destroyed");
        }
        return _passAll;

    }


    protected String testName() {

        return "QueryTest";
    }
}

public class QueryTest {

    public static void main(String[] args) {

        QyeryDriver cd = new QyeryDriver();
        boolean dbstatus;
        try {
            dbstatus = cd.runTests();

            if (dbstatus != true) {
                System.err.println("Error encountered during query:\n");
                Runtime.getRuntime().exit(1);
            }

            Runtime.getRuntime().exit(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}


