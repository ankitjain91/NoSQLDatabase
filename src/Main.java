import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import SSTablesImpl.*;

public class Main
{

    public static String compress(String str) throws Exception {
        if (str == null || str.length() == 0) {
            return str;
        }
        System.out.println("String length : " + str.length());
        ByteArrayOutputStream obj=new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(str.getBytes("UTF-8"));
        gzip.close();
        String outStr = obj.toString("UTF-8");
        System.out.println("Output String length : " + outStr.length());
        return outStr;
    }

    public static String decompress(String str) throws Exception {
        if (str == null || str.length() == 0) {
            return str;
        }
        System.out.println("Input String length : " + str.length());
        GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(str.getBytes("UTF-8")));
        BufferedReader bf = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
        String outStr = "";
        String line;
        while ((line=bf.readLine())!=null) {
            outStr += line;
        }
        System.out.println("Output String lenght : " + outStr.length());
        return outStr;
    }

    public static void main(String args[]) throws IOException {
        byte[] kompressed =compress("C-Nu");
        System.out.println(decompress(kompressed));
//
//        ArrayList<String> x = new ArrayList<>();
//        x.add("Id");
//        x.add("Name");
//        x.add("Surname");
//        String pKey = "Id";
//        String name = "Details";
//
//        Table.createTable(x, pKey, name);
//
//        Table t = Globals.inMemTables.get(name);
//
//        ArrayList<String> list = new ArrayList<>();
//        list.add("1");
//        list.add("Ankit");
//        list.add("Jain");
//        t.insertInTable(x, list);
//
//        list = new ArrayList<>();
//        list.add("2");
//        list.add("Rajat");
//        list.add("Pandey");
//        t.insertInTable(x,list);
//        t.flushTable();
//
//        list = new ArrayList<>();
//        list.add("3");
//        list.add("Ayushi");
//        list.add("Singh");
//        t.insertInTable(x,list);
//        ArrayList<String> colsName = new ArrayList<>();
//        colsName.add("Name");
//        ArrayList<String> colsValue = new ArrayList<>();
//        colsValue.add("Chhotu");
//        t = Globals.inMemTables.get(name);
//        t.update("3", colsName, colsValue);
//        t.flushTable();
//
//        colsName = new ArrayList<>();
//        colsName.add("Name");
//        colsValue = new ArrayList<>();
//        colsValue.add("Sadoo");
//        t = Globals.inMemTables.get(name);
//        t.update("1", colsName, colsValue);
//        t.flushTable();
//
//        t = Globals.inMemTables.get(name);
//        list = new ArrayList<>();
//        list.add("4");
//        list.add("anshu");
//        list.add("Rajendra");
//        t.insertInTable(x,list);
//        colsName = new ArrayList<>();
//        colsName.add("Surname");
//        t.delete(false, x , "2");
//        t.flushTable();
//
//        t = Globals.inMemTables.get(name);
//        list = new ArrayList<>();
//        list.add("2");
//        list.add("anshu");
//        list.add("Rajendra");
//        t.insertInTable(x,list);
//
//
//
//        t = Globals.inMemTables.get(name);
//        colsName = new ArrayList<>();
//        colsName.add("Surname");
//        colsValue = new ArrayList<>();
//        colsValue.add("Singhania");
//        t = Globals.inMemTables.get(name);
//        t.update("4", colsName, colsValue);
//
//        t = Globals.inMemTables.get(name);
//        try
//        {
//            t.merge();
//        }
//        catch (IOException e)
//        {
//            e.printStackTrace();
//        }
//
//        System.out.println("args = [" + args + "]");
    }

}
