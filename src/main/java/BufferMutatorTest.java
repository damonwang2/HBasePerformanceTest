import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NavigableMap;

public class BufferMutatorTest {

    private static Logger logger = LoggerFactory.getLogger(BufferMutatorTest.class);

    public static void main(String[] args) {

        try(Connection connection = ConnectionFactoryMy.createConnection()){
            boolean withWrite = false;
//            listenTest(connection);

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static long listenTest(Connection connection, String tableNameStr, boolean randomReading, boolean limitOne, boolean withWrite) {

        TableName tableName = TableName.valueOf(tableNameStr);

        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };

        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(listener);

//        自动关闭
        try(BufferedMutator mutator = connection.getBufferedMutator(tableName)){
//            Table table = connection.getTable(tableName);

            long timeStart = System.currentTimeMillis();

        }catch (Exception e){
            e.printStackTrace();
        }

        return 0;
    }


}
