package SSTablesImpl;

import BloomFilterUtils.MurmurHash;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static SSTablesImpl.Globals.inMemTables;
import static java.util.Arrays.*;

public class Table
{
    public  String tableName;
    private String primaryKey;
    private Map<String, ArrayList<ArrayList<String>>> cols;
    private Properties prop;
    private ArrayList<String> columnNames;

    public Table()
    {
        this.cols = new HashMap<>();
        this.prop = new Properties();
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setPrimaryKey(String primaryKey)
    {
        this.primaryKey = primaryKey;
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
            list.add(columnValues.get(i));
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
            ex.printStackTrace();
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
                    this.cols.get(columnNames.get(i)).get(offSet).set(0,updatedValues.get(i));
                    this.cols.get(columnNames.get(i)).get(offSet).set(1,System.currentTimeMillis()+"");
                    this.cols.get(columnNames.get(i)).get(offSet).set(2,"N");
                }
            }
        }

        else
        {
            listPrimaryKey.add(0, primaryKeyValue);
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
                        ex.printStackTrace();
                    }
                }
            }
        }
        return res;
    }

    public Map<String, String > read (ArrayList<String> columns, String primaryKey)
    {
        Map<String,String> res = new HashMap<>();
        ArrayList<ArrayList<String>> pkList = this.cols.get(this.primaryKey);
        boolean exists = false;

        if(columns.contains(this.primaryKey))
        {
            columns.remove(this.primaryKey);
        }

        for(int i = 0;  i < pkList.size(); i++)
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
        System.out.println("columns = [" + columns + "], primaryKey = [" + primaryKey + "]");
        return res;
    }

    public void merge() throws IOException {
        ArrayList<String> primaryKeyList = new ArrayList<>();
        ArrayList<String> tableNames = new ArrayList<>();
        Set<Object> keys;
        Map<String, String> res;
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
}
