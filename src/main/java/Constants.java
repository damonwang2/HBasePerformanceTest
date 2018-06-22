public class Constants {

    public static final String TABLE_T1 = "t2";

    public static final int TABLE_NUM = 20;

    public static final String TABLE_GET_PREFIX = "get";
    public static final String[] TABLE_GETS = new String[TABLE_NUM];

    public static final String TABLE_SCAN_PREFIX = "scan";
    public static final String[] TABLE_SCANS = new String[TABLE_NUM];

    public static final String COLUMN_FAMILY = "o";
    public static final byte[] COLUMN_FAMILY_BYTES = COLUMN_FAMILY.getBytes();

    public static final String QUALIFIER = "qualifier";
    public static final int QUALIFIER_NUM = 1;
    public static byte[][] qualifiers = new byte[QUALIFIER_NUM][];

    public static final String VALUE = "value";
    public static final byte[] VALUE_BYTES = VALUE.getBytes();
    public static final int VALUE_NUM = 5;
    public static String[] values = new String[VALUE_NUM];

    public static final int ID_NUM = 1000000;

    //初始化qualifiers数组和values数组
    static {
        for(int i = 0; i < QUALIFIER_NUM; i++){
            qualifiers[i] = (QUALIFIER + i).getBytes();
        }

        for(int i = 0; i < VALUE_NUM; i++){
            values[i] = VALUE + i;
        }

        for(int i = 0; i < TABLE_NUM; i++){
            TABLE_GETS[i] = TABLE_GET_PREFIX + (i+1);
            TABLE_SCANS[i] = TABLE_SCAN_PREFIX + (i+1);
        }
    }

}
