package SSTablesImpl;

public class Column
{
    private String name;
    private String value;
    private String timeStamp;
    private boolean tombStone;

    private Column(String name, String value, String timeStamp, boolean tombStone) {
        this.name = name;
        this.value = value;
        this.timeStamp = timeStamp;
        this.tombStone = tombStone;
    }

    public static Column getColumn(String name, String value, String timeStamp, boolean tombStone)
    {
        return new Column(name, value, timeStamp, tombStone);
    }
}
