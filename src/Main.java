import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import SSTablesImpl.*;

public class Main
{
    public static void main(String args[]) throws IOException {

        ArrayList<String> x = new ArrayList<>();
        x.add("Id");
        x.add("Name");
        x.add("Surname");
        String pKey = "Id";
        String name = "Details";

        Table.createTable(x, pKey, name);

        Table t = Globals.inMemTables.get("Details");

        ArrayList<String> list = new ArrayList<>();
        list.add("1");
        list.add("Ankit");
        list.add("Jain");
        t.insertInTable(x, list);

        list = new ArrayList<>();
        list.add("2");
        list.add("Rajat");
        list.add("Pandey");
        t.insertInTable(x,list);
        t.flushTable();

        x = new ArrayList<>();
        x.add("Id");
        x.add("Age");
        x.add("Salary");
        pKey = "Id";
        name = "Basics";

        Table.createTable(x, pKey, name);

        t = Globals.inMemTables.get("Basics");
        list = new ArrayList<>();
        list.add("1");
        list.add("29");
        list.add("4000");
        t.insertInTable(x, list);

        list = new ArrayList<>();
        list.add("2");
        list.add("30");
        list.add("5000");
        t.insertInTable(x,list);
        t.flushTable();

        ArrayList<String > cols1 = new ArrayList<>();
        cols1.add("Name");
        cols1.add("Surname");

        ArrayList<String > cols2 = new ArrayList<>();
        cols2.add("Age");
        cols2.add("Salary");

        Table.joinTable("Details", "Basics", cols1, cols2 );

//        list = new ArrayList<>();
//        list.add("3");
//        list.add("Ayushi");
//        list.add("Singh");
//        t.insertInTable(x,list);

        ArrayList<String> colsName = new ArrayList<>();
        colsName.add("Age");

        t.paginateAndRead(colsName, Table.compress("2"));
        t.paginateAndRead(colsName, Table.compress("2"));
        ArrayList<String> colsValue = new ArrayList<>();
        colsValue.add("Chhotu");
        t = Globals.inMemTables.get("Details");
        t.update("3", colsName, colsValue);
        t.flushTable();

        colsName = new ArrayList<>();
        colsName.add("Name");
        colsValue = new ArrayList<>();
        colsValue.add("Sadoo");
        t = Globals.inMemTables.get("Details");
        t.update("1", colsName, colsValue);
        t.flushTable();

        t = Globals.inMemTables.get("Details");
        list = new ArrayList<>();
        list.add("4");
        list.add("anshu");
        list.add("Rajendra");
        t.insertInTable(x,list);
        colsName = new ArrayList<>();
        colsName.add("Surname");
        t.delete(false, x , "2");
        t.flushTable();

        t = Globals.inMemTables.get("Details");
        list = new ArrayList<>();
        list.add("2");
        list.add("anshu");
        list.add("Rajendra");
        t.insertInTable(x,list);



        t = Globals.inMemTables.get("Details");
        colsName = new ArrayList<>();
        colsName.add("Surname");
        colsValue = new ArrayList<>();
        colsValue.add("Singhania");
        t = Globals.inMemTables.get("Details");
        t.update("4", colsName, colsValue);

        t = Globals.inMemTables.get("Details");
        try
        {
            t.merge();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        System.out.println("args = [" + args + "]");
    }

}
