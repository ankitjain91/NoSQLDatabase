package SSTablesImpl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Globals
{
    public static volatile ConcurrentHashMap<String,Table> inMemTables = new ConcurrentHashMap<>();
    public static final int MAX_TABlE_SIZE = 5;
    public static volatile Map<String, BitSet> bloomFilter = new TreeMap<>(); // Map to keep SStableIdentifier and its corresponding bit vector
    public static volatile ArrayList<String> flushedTables = new ArrayList<>();
}
