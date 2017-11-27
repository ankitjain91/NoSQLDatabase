package SSTablesImpl;

import BloomFilterUtils.MurmurHash;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static SSTablesImpl.Globals.inMemTables;
import static java.lang.Math.min;
import static java.util.Arrays.*;

public class Table
{
    public  String tableName;
    private String primaryKey;
    private Map<String, ArrayList<ArrayList<String>>> cols;
    private Properties prop;
    public ArrayList<String> columnNames;
    private Map<String,  Map<String, String>> cacheMap;

    public Table()
    {
        this.cols = new HashMap<>();
        this.prop = new Properties();
        this.cacheMap = new HashMap<>();
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setPrimaryKey(String primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    public static String compress(String string) {
//
//        if (string == null || string.length() == 0) {
//            return null;
//        }
//        try{
//            ByteArrayOutputStream obj=new ByteArrayOutputStream();
//            GZIPOutputStream gzip = new GZIPOutputStream(obj);
//            gzip.write(string.getBytes("UTF-8"));
//            gzip.close();
//            String outStr = obj.toString("UTF-8");
//            //System.out.println("Output String length : " + outStr.length());
//            //return obj.toByteArray();
//            return new String(Base64.getEncoder().encode(obj.toByteArray()));
//        }catch(Exception ex){
//
//        }
        return string;
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static String decompress(String st) {
//        try{
//            byte[] str = Base64.getDecoder().decode(st);
//            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(str));
//            BufferedReader bf = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
//            String outStr = "";
//            String line;
//            while ((line=bf.readLine())!=null) {
//                outStr += line;
//            }
//
//            return outStr;
//        }catch(Exception ex){
//            System.out.println("Error is : " + ex.getMessage());
//        }
//        return null;
        return st;
    }


    public static void createTable(ArrayList<String> columnNames, String primaryKey, String tableName)
    {
        short noOfCols = (short)columnNames.size();
        Table table = new Table();
        table.setPrimaryKey(primaryKey);
        table.setTableName(tableName);

        for(int i = 0; i < noOfCols; i++)
        {
            table.cols.put(columnNames.get(i), new ArrayList<ArrayList<String>>());
        }

        table.columnNames = columnNames;
        inMemTables.put(tableName, table);
    }

    public void insertInTable(ArrayList<String> columnNames, ArrayList<String> columnValues)
    {
        ArrayList<String> list;

        for(int i = 0; i < columnNames.size(); i++)
        {
            list = new ArrayList<>();
            list.add(compress(columnValues.get(i)));
            list.add(System.currentTimeMillis()+"");
            list.add("N"); // Always N for insert

            this.cols.get(columnNames.get(i)).add(list);
        }

        inMemTables.put(this.tableName, this);
    }

    private void createIndexAndFlush()
    {
        OutputStream out = null;
        OutputStream outName = null;
        String folderName = this.tableName+"_"+System.currentTimeMillis();
        File dir = new File(folderName);
        dir.mkdir();
        BitSet bit = new BitSet();

        try
        {
            int count  = 0;
            int size   = this.cols.get(this.primaryKey).size();
            String colProps = "";

            while(count < size)
            {
                colProps = "";
                for (String colName:this.cols.keySet())
                {
                    if ((colName != this.primaryKey))
                    {

                        String fName = colName + "_" + this.cols.get(this.primaryKey).get(count).get(0) + ".properties";
                        outName = new FileOutputStream(folderName+"/"+fName);

                        colProps += "," + fName;

                        Properties p = new Properties();
                        p.setProperty("Value", this.cols.get(colName).get(count).get(0));
                        p.setProperty("Timestamp", this.cols.get(colName).get(count).get(1));
                        p.setProperty("isTombstone", this.cols.get(colName).get(count).get(2));
                        p.store(outName, null);
                        outName.flush();
                        outName.close();

                        // Setting in the bloom filter bit vector
                        if(! this.cols.get(colName).get(count).get(0).equals("null"))
                        {
                            String valueToSet = this.cols.get(this.primaryKey).get(count).get(0) + colName;
                            bit.set(Math.abs(MurmurHash.hash32(valueToSet)));
                        }
                    }
                }

                if(this.prop.containsKey(this.cols.get(this.primaryKey).get(count).get(0)))
                {
                    String res = this.prop.getProperty(this.cols.get(this.primaryKey).get(count).get(0)) + ";"+colProps.substring(1);
                    this.prop.setProperty(this.cols.get(this.primaryKey).get(count).get(0), res);
                }
                else
                {
                    this.prop.setProperty(this.cols.get(this.primaryKey).get(count).get(0), colProps.substring(1));
                }
                count++;
            }

            out = new FileOutputStream(folderName+"/"+this.tableName + ".properties");
            this.prop.store(out, null);
            out.flush();
            out.close();
            Globals.bloomFilter.put(folderName,bit);
            Globals.inMemTables.remove(this.tableName);
            Globals.flushedTables.add(folderName);
            createTable(this.columnNames, this.primaryKey, this.tableName);
        }
        catch(Exception ex)
        {
            //ex.printStackTrace();
        }

    }

    public void flushTable()
    {
        createIndexAndFlush();
    }

    public void update(String primaryKeyValue, ArrayList<String> columnNames, ArrayList<String> updatedValues)
    {
        ArrayList<String> list = new ArrayList<>();
        ArrayList<String> listPrimaryKey = new ArrayList<>();
        ArrayList<ArrayList<String>> pKList = this.cols.get(this.primaryKey);

        ArrayList<ArrayList<String>> pkList = this.cols.get(this.primaryKey);
        boolean exists = false;

        for(int i = 0;  i < pkList.size(); i++)
        {
            if(pkList.get(i).get(0).equals(primaryKeyValue))
            {
                exists = true;
            }
        }

        if(exists)
        {
            int offSet = 0;
            ArrayList<ArrayList<String>> vals = this.cols.get(this.primaryKey);

            for(int i = 0 ; i < vals.size(); i++)
            {
                if(vals.get(i).get(0).equals(primaryKeyValue))
                {
                    break;
                }
                offSet++;
            }

            for(int i = 0; i < columnNames.size(); i++)
            {
                if(this.columnNames.contains(columnNames.get(i)) && (columnNames.get(i) != this.primaryKey))
                {
                    this.cols.get(columnNames.get(i)).get(offSet).set(0,compress(updatedValues.get(i)));
                    this.cols.get(columnNames.get(i)).get(offSet).set(1,System.currentTimeMillis()+"");
                    this.cols.get(columnNames.get(i)).get(offSet).set(2,"N");
                }
            }
        }

        else
        {
            listPrimaryKey.add(0, compress(primaryKeyValue));
            listPrimaryKey.add(System.currentTimeMillis()+"");
            listPrimaryKey.add("N");

            this.cols.get(this.primaryKey).add(listPrimaryKey);

            for(int i = 1; i < this.columnNames.size(); i++)
            {

                list = new ArrayList<>();

                if(columnNames.contains(this.columnNames.get(i)))
                {
                    list.add(updatedValues.get(columnNames.indexOf(this.columnNames.get(i))));
                    list.add(System.currentTimeMillis()+"");
                    list.add("N"); // Always N for insert
                }
                else
                {
                    int index = this.cols.get(this.columnNames.get(i)).size() - 1;

                    if(index >= 0)
                    {
                        ArrayList<String> l = this.cols.get(this.columnNames.get(i)).get(index);
                        list.add(l.get(0));
                    }
                    else
                    {
                        list.add("null");
                    }
                    list.add(System.currentTimeMillis()+"");
                    list.add("N"); // Always N for insert
                }

                this.cols.get(this.columnNames.get(i)).add(list);
            }
        }
        inMemTables.put(this.tableName, this);
    }

    public void delete(boolean fullRowDelete, ArrayList<String> columnNames, String primaryKey)
    {
        ArrayList<String> list;

        if(fullRowDelete)
        {
            for(int i = 0; i < this.columnNames.size() && this.columnNames.get(i) != this.primaryKey; i++)
            {
                list = new ArrayList<>();
                list.add("null");
                list.add(System.currentTimeMillis()+"");
                list.add("Y"); // Always N for insert
                this.cols.get(this.columnNames.get(i)).add(list);
            }
        }
        else
        {
            //Check if pk exists
            ArrayList<ArrayList<String>> pkList = this.cols.get(this.primaryKey);
            boolean exists = false;

            for(int i = 0;  i < pkList.size(); i++)
            {
                if(pkList.get(i).get(0).equals(primaryKey))
                {
                    exists = true;
                }
            }

            // If does not exists add primary key and set corresponding tombstone to yes
            if(!exists)
            {
                ArrayList<String> temp = new ArrayList<>();
                temp.add(primaryKey);
                temp.add(System.currentTimeMillis()+"");
                temp.add("N");

                this.cols.get(this.primaryKey).add(temp);

                for(int i = 0; i < this.columnNames.size(); i++)
                {
                    if(this.columnNames.get(i) != this.primaryKey)
                    {
                        if(columnNames.contains(this.columnNames.get(i)) )
                        {
                            temp = new ArrayList<>();
                            temp.add("null");
                            temp.add(System.currentTimeMillis() + "");
                            temp.add("Y");
                            this.cols.get(this.columnNames.get(i)).add(temp);
                        }
                        else
                        {
                            temp = new ArrayList<>();
                            temp.add("null");
                            temp.add(System.currentTimeMillis() + "");
                            temp.add("N");
                            this.cols.get(this.columnNames.get(i)).add(temp);
                        }
                    }
                }
            }
            //pk exists
            else
            {
                int offSet = 0;
                ArrayList<ArrayList<String>> vals = this.cols.get(this.primaryKey);

                for(int i = 0 ; i < vals.size(); i++)
                {
                    if(vals.get(i).get(0).equals(primaryKey))
                    {
                        break;
                    }
                    offSet++;
                }
                for(int i = 0; i < this.columnNames.size(); i++)
                {
                    if(columnNames.contains(this.columnNames.get(i)) && (this.columnNames.get(i) != this.primaryKey))
                    {
                        this.cols.get(this.columnNames.get(i)).get(offSet).set(0,"null");
                        this.cols.get(this.columnNames.get(i)).get(offSet).set(1,System.currentTimeMillis()+"");
                        this.cols.get(this.columnNames.get(i)).get(offSet).set(2,"Y");
                    }
                }
            }
        }
        inMemTables.put(this.tableName, this);
    }

    private Map<String,String> searchInMemTable(ArrayList<String> columns, String primaryKey)
    {
        Map<String, String> res = new HashMap<>();
        ArrayList<String> retrievedVals;
        retrievedVals = this.columnNames;//this.cols.get(this.primaryKey);

        int offSet = 0;
        ArrayList<ArrayList<String>> vals = this.cols.get(this.primaryKey);

        for(int i = 0 ; i < vals.size(); i++)
        {
            if(vals.get(i).get(0).equals(primaryKey))
            {
                break;
            }
            offSet++;
        }

        for(int i = 0; i < retrievedVals.size(); i++)
        {
            if(columns.contains(retrievedVals.get(i)))
            {
                if(this.cols.get(retrievedVals.get(i)).size() != 0 && !this.cols.get(retrievedVals.get(i)).get(offSet).get(0).isEmpty())
                {
                    if(!this.cols.get(retrievedVals.get(i)).get(offSet).get(0).equals("null"))
                    {
                        res.put(retrievedVals.get(i), this.cols.get(retrievedVals.get(i)).get(offSet).get(0));
                    }
                    else if (this.cols.get(retrievedVals.get(i)).get(offSet).get(2).equals("Y"))
                    {
                        res.put(retrievedVals.get(i), "null");
                    }

                }
            }
        }
        return res;
    }

    public Map<String, String> searchInSStables(ArrayList<String> columns, String primaryKey) {
        Map<String,String> res       = new HashMap<>();
        ArrayList<String> tableNames = new ArrayList<>();

        for(String table:Globals.flushedTables)
        {
            if(table.contains(this.tableName))
            {
                tableNames.add(table);
            }
        }

        Collections.sort(tableNames);
        Collections.reverse(tableNames);

        for(int i = 0; i < columns.size(); i++)
        {
            for(int j = 0; j < tableNames.size(); j++)
            {
                BitSet b = Globals.bloomFilter.get(tableNames.get(j));
                if(b.get(Math.abs(MurmurHash.hash32(primaryKey+columns.get(i)))))
                {
                    try
                    {
                        Properties properties = new Properties();
                        InputStream inputStream = new FileInputStream(tableNames.get(j)+"/"+columns.get(i)+"_"+primaryKey + ".properties");
                        properties.load(inputStream);
                        inputStream.close();
                        if(!res.containsKey(columns.get(i)))
                        {
                            res.put(columns.get(i), properties.getProperty("Value"));
                        }
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                    }
                }
                else
                {
                    try
                    {
                        Properties properties = new Properties();
                        InputStream inputStream = new FileInputStream(tableNames.get(j)+"/"+columns.get(i)+"_"+primaryKey + ".properties");
                        properties.load(inputStream);
                        if(!res.containsKey(columns.get(i)))
                        {
                            if(properties.getProperty("isTombstone").equals("Y"))
                            {
                                res.put(columns.get(i), "null");
                            }
                        }
                        inputStream.close();
                    }
                    catch (Exception ex)
                    {
                        //ex.printStackTrace();
                    }
                }
            }
        }
        return res;
    }

    public Map<String, String> readAll() throws IOException {
        //flushTable();
        ArrayList<String> pkList = getAllPrimaryKeys();
        Map<String, String> result = new HashMap<>();
        for (String key: pkList)
        {
                result.putAll(read(this.columnNames, key));
        }
        return result;
    }

    private Map<String, String > read (ArrayList<String> columns, String primaryKey) throws IOException {
        Map<String,String> res = new HashMap<>();
        Map<String , String > m = this.cacheMap.get(decompress(primaryKey));
        if(m != null && (m.size() >= columns.size()))
        {
            for(int i = 0; i < columns.size(); i++)
            {
                if(m.containsKey(columns.get(i)))
                {
                    res.put(columns.get(i), m.get(columns.get(i)));
                    if(res.size() == columns.size())
                    {
                        return res;
                    }
                }
                else {
                    res.clear();
                    break;
                }
            }
        }


        ArrayList<ArrayList<String>> pkList = this.cols.get(this.primaryKey);
        boolean exists = false;

        if(columns.contains(this.primaryKey))
        {
            columns.remove(this.primaryKey);
        }

        for(int i = 0;  pkList != null && i < pkList.size(); i++)
        {
            if(pkList.get(i).get(0).equals(primaryKey))
            {
                exists = true;
            }
        }

        if(exists)
        {
            res = searchInMemTable(columns,primaryKey);
        }

        Map<String,String> res2 = null;

        ArrayList<String> left = new ArrayList<>();
        if(res.size() != columns.size())
        {
            for(int i = 0; i < columns.size(); i++)
            {
                if(! res.containsKey(columns.get(i)))
                {
                    left.add(columns.get(i));
                }
            }

            if(left.size() > 0)
            {
                res2  = searchInSStables(left,primaryKey);
                res.putAll(res2);
            }
        }
        for (String key: res.keySet())
        {
            res.put(key,decompress(res.get(key)));
        }

        this.cacheMap.put(decompress(primaryKey), res);
        return res;
    }

    public Map<String, String > paginateAndRead(ArrayList<String> columns, String primaryKey) throws IOException {
        try {
            populateCache(columns);
            return read(columns, primaryKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    private void populateCache(ArrayList<String> columns) throws IOException {
        ArrayList<String> pkList = getAllPrimaryKeys();
        for(int  i = 0; i < min(pkList.size(), 10); i++)
        {
            read(columns, pkList.get(i));
        }
    }
    public synchronized void merge() throws IOException {
        ArrayList<String> primaryKeyList = new ArrayList<>();
        ArrayList<String> tableNames = new ArrayList<>();
        Set<Object> keys;
        Map<String, String> res;
        if(!this.columnNames.contains(this.primaryKey))
        {
            this.columnNames.add(this.primaryKey);
        }
        createTable(this.columnNames, this.primaryKey, this.tableName);
        Table t = Globals.inMemTables.get(this.tableName);
        ArrayList<String> colNameList = new ArrayList<>();
        ArrayList<String> colValueList =  new ArrayList<>();

        for(String table:Globals.flushedTables)
        {
            if(table.contains(this.tableName))
            {
                tableNames.add(table);
            }
        }
        Collections.sort(tableNames);
        Collections.reverse(tableNames);

        for(String table:tableNames)
        {
            if(table.contains(this.tableName))
            {
                Properties properties = new Properties();
                InputStream inputStream = new FileInputStream(table+"/"+this.tableName + ".properties");
                properties.load(inputStream);
                inputStream.close();
                keys = properties.keySet();
                for(Object k : keys)
                {
                    colNameList.clear();
                    colValueList.clear();
                    String primaryKey = (String)k;
                    if(! primaryKeyList.contains(primaryKey))
                    {
                        primaryKeyList.add(primaryKey);
                        colNameList.add(this.primaryKey);
                        colValueList.add(primaryKey);
                        res = read(this.columnNames, primaryKey);

                        for(String key : res.keySet())
                        {
                            colNameList.add(key);
                            colValueList.add(res.get(key));
                        }

                        int nullctr = 0;
                        for(String vals : colValueList)
                        {
                            if(vals.equals("null"))
                            {
                                nullctr++;
                            }
                        }

                        if(nullctr == colValueList.size()-1)
                        {
                            continue;
                        }
                        else
                        {
                            t.insertInTable(colNameList, colValueList);
                        }

                        int colOffset = 0;
                        for(String s  : colValueList)
                        {
                            if(s.equals("null"))
                            {
                                t.delete(false, new ArrayList<>(asList(colNameList.get(colOffset))),primaryKey);
                            }
                            colOffset++;
                        }
                    }
                }
            }
        }

        for(String folderName : tableNames)
        {
            File index = new File(folderName);
            String[]entries = index.list();

            for(String s: entries)
            {
                File currentFile = new File(index.getPath(),s);
                currentFile.delete();
            }
            index.delete();
        }
        t.flushTable();
    }

    public static Map<String, Map<String, String>> joinTable(String table1, String table2, ArrayList<String> columnList1, ArrayList<String> columnList2) throws IOException {
        Table t1 = Globals.inMemTables.get(table1);
        Table t2 = Globals.inMemTables.get(table2);
        ArrayList<String> pKList1 = t1.getAllPrimaryKeys();
        ArrayList<String> pKList2 = t2.getAllPrimaryKeys();

        List<String> commonPKeys = new ArrayList<String>(pKList1);
        commonPKeys.retainAll(pKList2);
        Map<String, Map<String, String>> result = new HashMap<>();
        Map<String,String> t1Res;

        for (String key: commonPKeys)
        {
            t1Res = (t1.read(columnList1, key));
            t1Res.putAll((t2.read(columnList2, key)));
            result.put(decompress(key), t1Res);
        }
        return result;
    }

    public ArrayList<String> getAllPrimaryKeys() throws IOException {
        ArrayList<String> primaryKeyList = new ArrayList<>();
        ArrayList<String> tableNames = new ArrayList<>();
        Set<Object> keys;
        Map<String, String> res;
        Table t = Globals.inMemTables.get(this.tableName);

        for(String table:Globals.flushedTables)
        {
            if(table.contains(this.tableName))
            {
                tableNames.add(table);
            }
        }
        Collections.sort(tableNames);
        Collections.reverse(tableNames);

        for(String table:tableNames) {
            if (table.contains(this.tableName)) {
                Properties properties = new Properties();
                InputStream inputStream = new FileInputStream(table + "/" + this.tableName + ".properties");
                properties.load(inputStream);
                inputStream.close();
                keys = properties.keySet();
                for (Object k : keys) {
                    String primaryKey = (String) k;
                    if (!primaryKeyList.contains(primaryKey)) {
                        primaryKeyList.add(primaryKey);
                    }
                }
            }
        }
        return primaryKeyList;
    }
}
