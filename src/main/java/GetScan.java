import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

public class GetScan{
    private static Random random = new Random();

    private static int countQueryEveryGroup = 1;
    private static int numGroup = 1;

    private static Logger logger = LoggerFactory.getLogger(GetScan.class);

    public static void main(String[] args) {

        try(Connection connection = ConnectionFactoryMy.createConnection()){
            boolean withWrite = false;
            scanGet(connection, withWrite);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void scanGet(Connection connection, boolean withWrite){

        //随机读
        boolean randomRead = false;

        //限制一行
        boolean limitOne = false;

        long timeScan = CustomMultiVersionScan(connection, Constants.TABLE_SCANS[9], randomRead, limitOne, withWrite);

        System.out.println(timeScan);

    }

    public static long CustomMultiVersionScan(Connection connection, String tableNameStr, boolean randomReading, boolean limitOne, boolean withWrite) {

        TableName tableName = TableName.valueOf(tableNameStr);

//        自动关闭
        try{
            Table tableCustomMultiVersion = connection.getTable(tableName);

            long timeStart = System.currentTimeMillis();

            //扫描一百次
            for(int i = 0; i < countQueryEveryGroup; i++){
                long curTime = System.currentTimeMillis();

                int id = i;
                if(randomReading){
                    id = random.nextInt(Constants.ID_NUM);
                }

                //根据id获取startKey,endKey等于其+1
                byte[] startRow = Utils.getRowKeyOf16(id).getBytes();
                byte[] stopRow = Utils.getNextRowKeyOf16(id).getBytes();

                Scan scan = new Scan();
                scan.withStartRow(startRow);
                scan.withStopRow(stopRow);
                scan.readAllVersions();

                if(limitOne){
                    scan.setOneRowLimit();
                }

                if(withWrite){
                    PutData.putCustomMultiVersion(connection, tableNameStr, Constants.VALUE_NUM, Constants.ID_NUM, 4);
                }

                ResultScanner resultScanner = tableCustomMultiVersion.getScanner(scan);
                Result result;

                while ( (result = resultScanner.next()) != null ){
                    NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = result.getMap();

                    //输出列族数量
                    System.out.println(map.size());

                    for (Map.Entry<byte[], NavigableMap<byte[],NavigableMap<Long,byte[]>>> entry: map.entrySet()) {
                        String cf = new String(entry.getKey());
                        NavigableMap<byte[],NavigableMap<Long,byte[]>> qualifiers= entry.getValue();

                        for(Map.Entry<byte[],NavigableMap<Long,byte[]>> quafilier : qualifiers.entrySet()){
                            String qualifierName = new String(quafilier.getKey());
                            System.out.println(qualifierName);

                            for(Map.Entry<Long, byte[]> timeValue : quafilier.getValue().entrySet()){
                                long timestamp = timeValue.getKey();
                                String value = new String(timeValue.getValue());
                                logger.info("time:value={}:{}", timestamp, value);
                            }
                        }
                    }

                }

            }

            long timeEnd = System.currentTimeMillis();

            long timeGap = timeEnd - timeStart;

            return timeGap;

        }catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }

    public static long HbaseMultiVersionGet(Connection connection, String tableNameStr, boolean randomReading, boolean limitOne, boolean withWrite){
        TableName tableName = TableName.valueOf(tableNameStr);

        ResultScanner results = null;
//        自动关闭
        try{
            Table tableCustomMultiVersion = connection.getTable(tableName);

            long timeStart = System.currentTimeMillis();

            //扫描一百次
            for(int i = 0; i < countQueryEveryGroup; i++){
                long curTime = System.currentTimeMillis();

                int id = i;
                if(randomReading){
                    id = random.nextInt(Constants.ID_NUM);
                }

                //根据id获取startKey,endKey等于其+1
                byte[] rowkey = Utils.getRowKeyOf16(id).getBytes();

                Get get = new Get(rowkey);
                //默认get一行,如果limitOne=false，则读取所有的版本
                if(!limitOne){
                    get.readAllVersions();
                }

                if(withWrite){
                    PutData.putCustomMultiVersion(connection, tableNameStr, Constants.VALUE_NUM, Constants.ID_NUM, 4);
                }

                tableCustomMultiVersion.get(get);
            }

            long timeEnd = System.currentTimeMillis();

            long timeGap = timeEnd - timeStart;

            return timeGap;

        }catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }

}
