import java.io.*;
import java.util.*;
import QueryManager.Queries;
import SSTablesImpl.*;

public class Main {
    public static void createTable(String query) {
        System.out.println(query);
        String[] splited = query.split("\\s+");
        ArrayList<String> columnNames = new ArrayList<String>();
        String tableName = splited[2];
        ArrayList<String> cols = getColumnList(query);
        String primaryKey = splited[splited.length - 1];
        Table.createTable(new ArrayList<>(cols), primaryKey, tableName);
    }

    private static void insertTable(String query) {
        System.out.println(query);
        String[] splited = query.split("\\s+");
        String tableName = splited[2];
        Table t = Globals.inMemTables.get(tableName);

        ArrayList<String> cols = getColumnList(query);
        ArrayList<String> colVals = getColumnList(query.substring(query.indexOf(")") + 1));
        t.insertInTable(cols, colVals);
    }

    private static void updateTable(String query)
    {
        System.out.println(query);
        String[] splited = query.split("\\s+");
        String tableName = splited[2];
        Table t = Globals.inMemTables.get(tableName);

        ArrayList<String> cols = getColumnList(query);
        ArrayList<String> colVals = getColumnList(query.substring(query.indexOf(")") + 1));
        String primaryKey = splited[splited.length - 1];
        t.update(primaryKey, cols, colVals);
    }

    private static void deleteTable(String query)
    {
        System.out.println(query);
        String[] splited = query.split("\\s+");
        String tableName = splited[2];
        Table t = Globals.inMemTables.get(tableName);

        ArrayList<String> cols = getColumnList(query);
        String primaryKey = splited[splited.length - 1];
        t.delete(false, cols, primaryKey);
    }

    private synchronized static void joinTable(String query) throws IOException {
        System.out.println(query);
        //"Select from Details (Surname,Age) AND from Cartoons (Character) where Details.Name = Cartoons.Name";
        String[] splited = query.split("\\s+");

        String tableName1 = splited[2];
        String tableName2 = splited[6];

        Table t1 = Globals.inMemTables.get(tableName1);
        t1.flushTable();

        Table t2 = Globals.inMemTables.get(tableName2);
        t2.flushTable();

        ArrayList<String> cols1   = getColumnList(query);
        ArrayList<String> colVals = getColumnList(query.substring(query.indexOf(")") + 1));
        Map<String, Map<String, String>> res =  t1.joinTable(tableName1,tableName2,cols1,colVals);
        for(String key : res.keySet())
        {
            print(res.get(key));
        }
    }


    private static void readTable(String query) throws IOException {
        System.out.println(query);
        String[] splited = query.split("\\s+");
        String tableName = splited[2];
        Table t = Globals.inMemTables.get(tableName);
        ArrayList<String> cols;

        if(query.contains("*"))
        {
            print(t.readAll());
        }
        else
        {
            cols = getColumnList(query);
            String primaryKey = splited[splited.length - 1];
            print(t.paginateAndRead(cols, primaryKey));
        }
    }


    private static ArrayList<String> getColumnList(String query)
    {

        int startIndex = query.indexOf('(');
        int endIndex   = query.indexOf(')');

        String substr = query.substring(startIndex+1,endIndex);

        String[] col;
        if(substr.contains(","))
        {
            col = substr.split(",");
        }
        else
        {
            col = new String[]{substr};
        }

        List<String> colList = Arrays.asList(col);
        return new ArrayList<>(colList);
    }

    static void print(Map<String, String> m)
    {
        if(m == null)
        {
            return;
        }

        for(String key : m.keySet())
        {
            System.out.println(key + " - " + m.get(key));
        }
    }

    public static void main(String args[]) throws IOException
    {
        try
        {
            createTable(Queries.CREATE_TABLE1);
            insertTable(Queries.INSERT_TABLE1);
            insertTable(Queries.INSERT_TABLE3);
            insertTable(Queries.INSERT_TABLE4);
            Table t = Globals.inMemTables.get("Details");
            t.flushTable();

            createTable(Queries.CREATE_TABLE2);
            insertTable(Queries.INSERT_TABLE2);

            t = Globals.inMemTables.get("Cartoons");
            t.flushTable();

            updateTable(Queries.UPDATE_TABLE);
            deleteTable(Queries.DELETE_TABLE);
            t = Globals.inMemTables.get("Details");
            t.flushTable();


            readTable(Queries.READ_ALL_TABLE);
            joinTable(Queries.JOIN_TABLE);

            for(String key : Globals.inMemTables.keySet())
            {
                t = Globals.inMemTables.get(key);
                synchronized (t)
                {
                    t.merge();
                }
            }


        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }
}
