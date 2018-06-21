import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class Test {
    public static void main(String[] args) {

        Integer i = 5;

        try(Connection connection = ConnectionFactoryMy.createConnection()){

//            scanTest(connection, Constants.TABLE_SCANS[2], true);
//
//            scanTest(connection, Constants.TABLE_SCANS[2], false);
        }catch (Exception e){

        }
    }

    public static void scanTest(Connection connection, String tableName, boolean visit){
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));

            table.getScanner(new Scan().withStartRow("0".getBytes()).withStopRow("0".getBytes()));

            long startTime = System.currentTimeMillis();

            int count = 0;
            for(int i = 0; i < 1000; i++){
                byte[] startRow = Utils.getRowKeyOf16(i).getBytes();
                byte[] stopRow = Utils.getNextRowKeyOf16(i).getBytes();

                Scan scan = new Scan();
                scan.withStartRow(startRow);
                scan.withStopRow(stopRow);
                scan.setLimit(1);
                scan.setBatch(1);

                ResultScanner results = table.getScanner(scan);

                for(Result result : results){
                    count++;
                }
            }

            System.out.println("一共next" + count + "次");

            long endTime = System.currentTimeMillis();

            long scanTime = endTime - startTime;

            if(visit){
                System.err.println("scan and visit1000次:" + scanTime + "ms");
            }else {
                System.err.println("scan1000次:" + scanTime + "ms");
            }


        }catch (Exception e){

        }
    }
}
