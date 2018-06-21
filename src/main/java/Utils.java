import java.util.Arrays;

public class Utils {
    public static String getRowKeyOf16(int id){
        if(id < 0){
            return "negative";
        }
        int prefix = id % 10;

        String idStr = String.valueOf(id);

        char[] zeros = new char[16-1-idStr.length()];
        Arrays.fill(zeros, '0');

        return prefix + new String(zeros) + idStr;
    }

    public static String getRowKeyOf32(int id, long timestamp) {

        String timeStr = String.valueOf(timestamp);

        char[] zeros = new char[16-timeStr.length()];
        Arrays.fill(zeros, '0');
        
        return getRowKeyOf16(id) + new String(zeros) + timeStr;
    }

    public static String getNextRowKeyOf16(int id) {
        if(id < 0){
            return "negative";
        }
        int prefix = id % 10;

        String idStr = String.valueOf(id+1);

        char[] zeros = new char[16-1-idStr.length()];
        Arrays.fill(zeros, '0');

        return prefix + new String(zeros) + idStr;
    }


    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        System.out.println(getRowKeyOf32(125, time));

        System.out.println(getRowKeyOf16(125));

        System.out.println(getNextRowKeyOf16(125));
    }
}
