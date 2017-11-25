package SSTablesImpl;

import java.util.*;

public class Globals
{
    public static HashMap<String,Table> inMemTables = new HashMap<>();
    public static final int MAX_TABlE_SIZE = 5;
    public static Map<String, BitSet> bloomFilter = new TreeMap<>(); // Map to keep SStableIdentifier and its corresponding bit vector
    public static ArrayList<String> flushedTables = new ArrayList<>();
}
