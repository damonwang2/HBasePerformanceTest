import org.apache.commons.math3.analysis.function.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.ArrayList;
import java.util.List;

public class PutData {
    private static Admin admin;

    public static void main(String[] args) {

        putBatch();

    }

    public static void putBatch(){

        try(Connection connection = ConnectionFactoryMy.createConnection()){

            for(int i = 0; i < Constants.TABLE_NUM; i++){

                //自定义多版本
                putCustomMultiVersion(connection, Constants.TABLE_SCANS[i], i+1, 0, Constants.ID_NUM);

                //hbase多版本
                putHbaseMultiVersion(connection, Constants.TABLE_GETS[i], i+1, 0, Constants.ID_NUM);

                System.out.println("put table" + (i+1) + "pairs");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void putCustomMultiVersion(Connection connection, String tableNameStr, int versionNum, int startId, int length){
        try{
            TableName tableName = TableName.valueOf(tableNameStr);

            Table tableCustomMultiVersion = connection.getTable(tableName);

            for(int k = 0; k < versionNum; k++){

                List<Put> puts = new ArrayList<>();
                int count = 0;

                for (int i = startId; i < length; i++){

                    long time = System.currentTimeMillis();
                    byte[] row = Utils.getRowKeyOf32(i, time).getBytes();

                    Put put = new Put(row);
                    //插入10列
                    for(int j = 0; j < Constants.QUALIFIER_NUM; j++){
                        put.addColumn(Constants.COLUMN_FAMILY_BYTES, Constants.qualifiers[j], Constants.VALUE_BYTES);
                    }

                    puts.add(put);

                    //每隔10000put一次，一共100次
                    if(puts.size() >= 10000 || i == (startId+length-1)){
                        tableCustomMultiVersion.put(puts);
                        Thread.sleep(50);
                        puts.clear();
                    }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void putHbaseMultiVersion(Connection connection, String tableNameStr, int versionNum, int startId, int length) {
        try{

            TableName tableName = TableName.valueOf(tableNameStr);

            Table tableHbaseMultiVersion = connection.getTable(tableName);

            //多版本插入，分多次，错开时间戳
            for(int k = 0; k < versionNum; k++) {
                List<Put> puts = new ArrayList<>();
                int count = 0;

                for (int i = startId; i < length; i++) {

                    long time = System.currentTimeMillis();
                    byte[] row = Utils.getRowKeyOf16(i).getBytes();

                    Put put = new Put(row);
                    //插入10列
                    for (int j = 0; j < Constants.QUALIFIER_NUM; j++) {
                        put.addColumn(Constants.COLUMN_FAMILY_BYTES, Constants.qualifiers[j], Constants.VALUE_BYTES);
                    }

                    puts.add(put);

                    //每隔10000put一次，一共100次
                    if (puts.size() >= 10000 || i == (startId+length-1) ) {
                        tableHbaseMultiVersion.put(puts);
                        Thread.sleep(50);
                        puts.clear();
                    }
                }

                System.out.println("put 1M");
            }
        }catch (Exception e){

        }
    }
}
