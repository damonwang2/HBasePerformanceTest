import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.Iterator;
import java.util.Random;

public class GetScan{
    private static Random random = new Random();

    private static int countQueryEveryGroup = 10000;
    private static int numGroup = 1;
    private static int countQuerySum = numGroup * countQueryEveryGroup;

    public static void main(String[] args) {

        try(Connection conn = ConnectionFactoryMy.createConnection()){
            boolean withWrite = false;
//            scanGet(conn, withWrite);

            withWrite = true;
            scanGet(conn, withWrite);

        }catch (Exception e){

        }
    }

    public static void scanGet(Connection conn, boolean withWrite){
        //第一次获取数据会耗时，我们先获取，保证后面对比的公平

        HbaseMultiVersionGet(conn, Constants.TABLE_GETS[Constants.TABLE_NUM-1], true, true,true);

        long[] sumScanNoLimit = new long[20];
        long[] sumScanOneLimit = new long[20];
        long[] sumGetOneLimit = new long[20];
        long[] sumGetNoLimit = new long[20];

        //随机读
        boolean randomRead = true;

        //限制一行
        boolean limitOne = false;

        for(int j = 0; j < numGroup; j++){
            System.out.println("group" + j);
            for(int i = 0; i < Constants.TABLE_NUM; i++){
                limitOne = false;
                long timeScan = CustomMultiVersionScan(conn, Constants.TABLE_SCANS[i], randomRead, limitOne, withWrite);
                sumScanNoLimit[i] += timeScan;
            }
            System.out.println("scan1");

            for(int i = 0; i < Constants.TABLE_NUM; i++){
                limitOne = true;
                long timeScan = CustomMultiVersionScan(conn, Constants.TABLE_SCANS[i], randomRead, limitOne, withWrite);
                sumScanOneLimit[i] += timeScan;
            }
            System.out.println("scan2");

            for(int i = 0; i < Constants.TABLE_NUM; i++){
                limitOne = false;
                long timeGet = HbaseMultiVersionGet(conn, Constants.TABLE_GETS[i], randomRead, limitOne, withWrite);
                sumGetNoLimit[i] += timeGet;
            }
            System.out.println("get1");

            for(int i = 0; i < Constants.TABLE_NUM; i++){
                limitOne = true;
                long timeGet = HbaseMultiVersionGet(conn, Constants.TABLE_GETS[i], randomRead, limitOne, withWrite);
                sumGetOneLimit[i] += timeGet;
            }
            System.out.println("get2");

        }

        for(int i = 0; i < Constants.TABLE_NUM; i++){
            System.out.print((double)sumScanNoLimit[i] / (countQuerySum) + " ");
            System.out.print((double)sumScanOneLimit[i] / (countQuerySum) + " ");
            System.out.print((double)sumGetNoLimit[i] / (countQuerySum) + " ");
            System.out.print((double)sumGetOneLimit[i] / (countQuerySum) + " ");
            System.out.println();
        }
    }


    public static long CustomMultiVersionScan(Connection conn, String tableNameStr, boolean randomReading, boolean limitOne, boolean withWrite) {

        TableName tableName = TableName.valueOf(tableNameStr);

//        自动关闭
        try{
            Table tableCustomMultiVersion = conn.getTable(tableName);

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
                if(limitOne){
                    scan.setOneRowLimit();
                }

                if(withWrite){
                    PutData.putCustomMultiVersion(conn, tableNameStr, Constants.VALUE_NUM, Constants.ID_NUM, 4);
                }

                ResultScanner resultScanner = tableCustomMultiVersion.getScanner(scan);
                Iterator<Result> resultIterator = resultScanner.iterator();

                while (resultIterator.hasNext()){
                    resultIterator.next();
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

    public static long HbaseMultiVersionGet(Connection conn, String tableNameStr, boolean randomReading, boolean limitOne, boolean withWrite){
        TableName tableName = TableName.valueOf(tableNameStr);

        ResultScanner results = null;
//        自动关闭
        try{
            Table tableCustomMultiVersion = conn.getTable(tableName);

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
                    PutData.putCustomMultiVersion(conn, tableNameStr, Constants.VALUE_NUM, Constants.ID_NUM, 4);
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
